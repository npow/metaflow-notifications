"""
NotifyStepDecorator and NotifyFlowDecorator for metaflow-notifications.
"""

import atexit
import ipaddress
import os
import unicodedata
import warnings
from concurrent.futures import ThreadPoolExecutor
from concurrent.futures import TimeoutError as _FuturesTimeoutError
from urllib.parse import unquote

from metaflow.decorators import FlowDecorator, StepDecorator

from . import _message

# Module-level set for foreach warn-once: keyed on (run_id, step_name).
# Capped at _FOREACH_WARNED_MAX entries to prevent unbounded growth in
# long-running orchestrators that process thousands of flows.
_FOREACH_WARNED_MAX = 10_000
_foreach_warned = set()

# Remote execution decorator names that cause notifications to be silenced on workers
_REMOTE_DECORATORS = {"kubernetes", "batch"}

# Shared defaults — single source of truth for both decorator classes
_NOTIFY_DEFAULTS = {
    "on_failure": True,
    "on_success": False,
    "on_start": False,
    "notifier": None,
    "timeout": 10,
    "message": True,
    "title": True,
}

# URL schemes that are blocked for security reasons.
# http and https are permitted but subject to SSRF IP-range checks.
# NOTE: A scheme-based denylist cannot enumerate every dangerous scheme exhaustively.
# If stricter security is required, consider setting METAFLOW_NOTIFY_URLS to only
# well-known notification service URLs in your deployment environment.
_BLOCKED_SCHEMES = frozenset(
    [
        # Code/command execution
        "exec",
        "cmd",
        "shell",
        "python",
        "vbscript",
        "javascript",
        # File system access
        "file",
        # SSRF / protocol attack primitives
        "ftp",
        "ftps",
        "sftp",
        "smb",
        "cifs",
        "ldap",
        "ldaps",
        "ldapi",
        "gopher",
        "dict",
        # Data URIs (can carry local content or trigger local execution)
        "data",
        # Java archive (remote class loading)
        "jar",
        # Rarely-used but dangerous
        "netdoc",
    ]
)

# Private and link-local IP networks — blocked for SSRF protection on http/https URLs.
_PRIVATE_NETWORKS = (
    ipaddress.ip_network("127.0.0.0/8"),  # Loopback
    ipaddress.ip_network("10.0.0.0/8"),  # RFC1918 private
    ipaddress.ip_network("172.16.0.0/12"),  # RFC1918 private
    ipaddress.ip_network("192.168.0.0/16"),  # RFC1918 private
    ipaddress.ip_network("169.254.0.0/16"),  # Link-local / AWS IMDSv1
    ipaddress.ip_network("::1/128"),  # IPv6 loopback
    ipaddress.ip_network("fc00::/7"),  # IPv6 unique-local
    ipaddress.ip_network("fe80::/10"),  # IPv6 link-local
)

# Thread pool for notification dispatch
_executor = ThreadPoolExecutor(max_workers=4, thread_name_prefix="mf-notify")
atexit.register(_executor.shutdown, wait=False)

# Apprise's own logger is noisy at DEBUG. Set it to WARNING once at import time.
# We only set this if the apprise logger hasn't been explicitly configured already.
_apprise_logger_configured = False


def _check_apprise():
    """Return True if apprise is importable, False otherwise."""
    try:
        import apprise  # noqa: F401

        return True
    except ImportError:
        return False


def _is_private_host(hostname):
    """Return True if hostname is a private or link-local IP address."""
    try:
        addr = ipaddress.ip_address(hostname)
        return any(addr in net for net in _PRIVATE_NETWORKS)
    except ValueError:
        return False


def _extract_scheme(url):
    """
    Normalize a URL and extract its scheme.

    Handles bypass vectors:
    - NFKC normalization: Unicode lookalike characters (e.g. ｆｉｌｅ → file)
    - Percent-decoding: %3A colon bypass (file%3A///etc → file://...)
    - Whitespace stripping: "file ://" → "file://"
    - data: URIs (no //) and other schemeless formats

    Returns (scheme_lower_str, url_normalized_str).
    scheme is empty string if no scheme can be extracted.
    """
    # Step 1: NFKC normalization catches Unicode lookalikes
    url_nfkc = unicodedata.normalize("NFKC", url)
    # Step 2: Percent-decode to catch %3A colon bypass
    url_decoded = unquote(url_nfkc)
    # Step 3: Strip leading/trailing whitespace
    url_clean = url_decoded.strip()

    if "://" in url_clean:
        # Standard scheme://authority format
        scheme = url_clean.split("://", 1)[0].lower().strip()
    elif ":" in url_clean:
        # Possibly data:mime/type or similar (no //).
        # Only extract if the prefix looks like a valid scheme (letters/digits/+/-/. only)
        prefix = url_clean.split(":", 1)[0]
        if prefix and all(c.isalnum() or c in "+-.+" for c in prefix):
            scheme = prefix.lower().strip()
        else:
            scheme = ""
    else:
        scheme = ""

    return scheme, url_clean


def _dispatch(urls, *, title, body, timeout=10):
    """
    Send notification via Apprise. Blocks for up to timeout seconds.
    Non-fatal: emits warnings on timeout, delivery failure, or unrecognized URLs.
    Does not expose credential URLs in warning messages.
    """
    global _apprise_logger_configured

    import logging

    import apprise

    if not _apprise_logger_configured:
        logging.getLogger("apprise").setLevel(logging.WARNING)
        _apprise_logger_configured = True

    if not urls:
        return

    ap = apprise.Apprise()
    skipped = 0
    for url in urls:
        if not ap.add(url):
            skipped += 1
    if skipped:
        warnings.warn(
            f"metaflow-notifications: {skipped} URL(s) were not recognized by Apprise and will be skipped "
            "(check URL format and scheme)",
            stacklevel=2,
        )

    try:
        fut = _executor.submit(ap.notify, title=title, body=body)
    except Exception:  # noqa: BLE001
        warnings.warn(
            "metaflow-notifications: could not schedule notification (executor may be shut down)",
            stacklevel=2,
        )
        return

    try:
        result = fut.result(timeout=timeout)
    except _FuturesTimeoutError:
        warnings.warn(
            f"metaflow-notifications: notification did not complete within {timeout}s timeout",
            stacklevel=2,
        )
        return
    except KeyboardInterrupt:
        fut.cancel()
        raise
    except Exception as exc:  # noqa: BLE001
        warnings.warn(
            f"metaflow-notifications: notification raised an exception: {type(exc).__name__}",
            stacklevel=2,
        )
        return

    if not result:
        warnings.warn(
            "metaflow-notifications: notification dispatch returned a falsy result (delivery may have failed)",
            stacklevel=2,
        )


class NotifyStepDecorator(StepDecorator):
    name = "notify"
    defaults = dict(_NOTIFY_DEFAULTS)

    def init(self):
        if getattr(self, "_ran_init", False):
            return
        self._ran_init = True
        self._run_id = None
        self._task_id = "0"
        self._flow_name = "UnknownFlow"
        self._apprise_available = None
        self._is_foreach_worker = False

    def step_init(self, flow, graph, step_name, decorators, environment, flow_datastore, logger):
        self.logger = logger
        self._apprise_available = _check_apprise()
        if not self._apprise_available:
            warnings.warn(
                "metaflow-notifications: apprise is not installed; notifications disabled. "
                "Install with: pip install apprise",
                stacklevel=2,
            )

    def task_pre_step(
        self,
        step_name,
        task_datastore,
        metadata,
        run_id,
        task_id,
        flow,
        graph,
        retry_count,
        max_user_code_retries,
        ubf_context,
        inputs,
    ):
        self._run_id = run_id
        self._task_id = str(task_id) if task_id is not None else "0"
        self._flow_name = flow.__class__.__name__

        # Foreach fan-out: only notify from the control task (ubf_context is None)
        if ubf_context is not None:
            self._is_foreach_worker = True
            key = (run_id, step_name)
            if key not in _foreach_warned:
                # Cap the set to prevent unbounded growth in long-running orchestrators
                if len(_foreach_warned) >= _FOREACH_WARNED_MAX:
                    _foreach_warned.clear()
                _foreach_warned.add(key)
                warnings.warn(
                    f"metaflow-notifications: step '{step_name}' (run {run_id}) runs in foreach fan-out. "
                    "Notifications will only fire from the control task, not worker tasks.",
                    stacklevel=2,
                )
            return

        spec = self.attributes.get("on_start", False)
        if not spec:
            return

        # Retry guard: only notify on the first attempt
        if retry_count > 0:
            return

        self._fire(step_name, self._run_id, "start", spec, error=None)

    def task_post_step(
        self,
        step_name,
        flow,
        graph,
        retry_count,
        max_user_code_retries,
    ):
        if self._is_foreach_worker:
            return
        spec = self.attributes.get("on_success", False)
        if not spec:
            return
        self._fire(step_name, self._run_id, "success", spec, error=None)

    def task_exception(
        self,
        exception,
        step_name,
        flow,
        graph,
        retry_count,
        max_user_code_retries,
    ):
        if self._is_foreach_worker:
            return

        # Suppress if Metaflow's @catch already handled it
        if type(exception).__name__ == "FailureHandledByCatch":
            return

        # Only notify on the final failure (no more retries remaining).
        # max_user_code_retries == -1 means unlimited retries; fire on every exception.
        if max_user_code_retries != -1 and retry_count < max_user_code_retries:
            return

        spec = self.attributes.get("on_failure", True)
        if not spec:
            return

        self._fire(step_name, self._run_id, "failure", spec, error=exception)

    def _resolve_urls(self):
        """Return list of notification URLs, checking attribute then env var."""
        notifier = self.attributes.get("notifier")
        if not notifier:
            notifier = os.environ.get("METAFLOW_NOTIFY_URLS")
        if not notifier:
            warnings.warn(
                "metaflow-notifications: no notifier URL configured. "
                "Set notifier= on @notify or METAFLOW_NOTIFY_URLS env var.",
                stacklevel=3,
            )
            return []
        if isinstance(notifier, str):
            raw_urls = [notifier]
        else:
            raw_urls = list(notifier)

        safe = []
        for url in raw_urls:
            scheme, _url_clean = _extract_scheme(url)

            if not scheme:
                warnings.warn(
                    "metaflow-notifications: a notifier URL has no recognizable scheme and was skipped",
                    stacklevel=3,
                )
                continue

            if scheme in _BLOCKED_SCHEMES:
                warnings.warn(
                    f"metaflow-notifications: URL scheme '{scheme}://' is blocked for security reasons",
                    stacklevel=3,
                )
                continue

            # SSRF protection: block private/link-local IP targets for http/https
            if scheme in ("http", "https") and "://" in _url_clean:
                rest = _url_clean.split("://", 1)[1]
                host_part = rest.split("/")[0].split("?")[0].split("#")[0]
                if host_part.startswith("["):  # IPv6 literal
                    hostname = host_part[1:].split("]")[0]
                else:
                    hostname = host_part.split(":")[0]
                if _is_private_host(hostname):
                    warnings.warn(
                        "metaflow-notifications: URL targets a private or link-local IP address "
                        "and was blocked (SSRF protection)",
                        stacklevel=3,
                    )
                    continue

            safe.append(url)
        return safe

    def _fire(self, step_name, run_id, event, spec, *, error=None):
        """
        Resolve URLs, build message, dispatch notification.

        Security note: When spec is callable, the live exception object is passed to it
        with its __traceback__ intact. Python tracebacks retain references to all local
        variables in every stack frame at the time of the exception. Use callable specs
        only with trusted code; do not pass user-supplied callables in environments where
        local variable exposure (passwords, tokens, keys) is a concern.
        """
        try:
            if not self._apprise_available:
                return

            urls = self._resolve_urls()
            if not urls:
                return

            # Use None-aware check instead of truthiness to handle run_id=0 or run_id="0"
            safe_run_id = run_id if run_id is not None else "?"

            msg = _message.resolve_message(
                spec,
                event=event,
                flow_name=self._flow_name,
                run_id=safe_run_id,
                step_name=step_name,
                task_id=self._task_id,
                error=error,
            )
            if msg is None:
                return

            # Build title
            title_spec = self.attributes.get("title", True)
            title = (
                _message.resolve_message(
                    title_spec,
                    event=event,
                    flow_name=self._flow_name,
                    run_id=safe_run_id,
                    step_name=step_name,
                    task_id=self._task_id,
                    error=error,
                )
                or f"Metaflow: {self._flow_name}"
            )

            timeout = self.attributes.get("timeout", 10)
            _dispatch(urls, title=title, body=msg, timeout=timeout)
        except Exception as exc:  # noqa: BLE001
            warnings.warn(
                f"metaflow-notifications: unexpected error in _fire "
                f"({type(exc).__name__}: {exc}); notification suppressed",
                stacklevel=2,
            )


class NotifyFlowDecorator(FlowDecorator):
    name = "notify"
    defaults = dict(_NOTIFY_DEFAULTS)

    def flow_init(self, flow, graph, environment, flow_datastore, metadata, logger, echo, options):
        self.logger = logger
        notifier = self.attributes.get("notifier")
        apprise_available = _check_apprise()

        for node in graph:
            existing_names = {d.name for d in node.decorators}

            # Check for conflict: step has @notify(on_failure=False) but flow has on_failure=True
            if "notify" in existing_names and self.attributes.get("on_failure"):
                for d in node.decorators:
                    if d.name == "notify" and d.attributes.get("on_failure") is False:
                        warnings.warn(
                            f"metaflow-notifications: step '{node.name}' has @notify(on_failure=False) "
                            "but the flow-level @notify has on_failure=True. "
                            "The step decorator will suppress failure notifications for this step.",
                            stacklevel=2,
                        )

            # Warn about remote execution if URL is configured
            if notifier:
                remote_deco_names = existing_names & _REMOTE_DECORATORS
                if remote_deco_names:
                    remote_name = next(iter(remote_deco_names))
                    warnings.warn(
                        f"metaflow-notifications: step '{node.name}' uses @{remote_name} (remote execution). "
                        "Notifications run on the remote worker and may not reach your configured URL "
                        "unless credentials are available in the remote environment.",
                        stacklevel=2,
                    )

            # Skip steps that already have @notify
            if "notify" in existing_names:
                continue

            # Inject a step decorator with flow-level defaults
            step_deco = NotifyStepDecorator()
            step_deco.attributes = dict(self.attributes)
            step_deco._ran_init = True
            step_deco._run_id = None
            step_deco._task_id = "0"
            step_deco._flow_name = flow.__class__.__name__
            step_deco._apprise_available = apprise_available
            step_deco._is_foreach_worker = False
            step_deco.logger = logger

            node.decorators.append(step_deco)
