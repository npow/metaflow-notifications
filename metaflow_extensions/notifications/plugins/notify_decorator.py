"""
NotifyStepDecorator and NotifyFlowDecorator for metaflow-notifications.
"""
import os
import warnings
from concurrent.futures import ThreadPoolExecutor, TimeoutError as _FuturesTimeoutError

from metaflow.decorators import FlowDecorator, StepDecorator

from . import _message

# Module-level set for foreach warn-once: keyed on (run_id, step_name)
_foreach_warned = set()

# Remote execution decorator names that cause notifications to be silenced on workers
_REMOTE_DECORATORS = {"kubernetes", "batch"}

# Map event name → attribute key for message spec lookup
_EVENT_ATTR = {
    "failure": "on_failure",
    "success": "on_success",
    "start": "on_start",
}

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

# Thread pool for non-blocking notification dispatch
_executor = ThreadPoolExecutor(max_workers=4, thread_name_prefix="mf-notify")


def _check_apprise():
    """Return True if apprise is importable, False otherwise."""
    try:
        import apprise  # noqa: F401
        return True
    except ImportError:
        return False


def _dispatch(urls, *, title, body, timeout=10):
    """
    Send notification via Apprise on a thread-pool thread.
    Non-fatal: logs warnings on timeout or delivery failure.
    Does not expose credentials in warnings.
    """
    import apprise

    ap = apprise.Apprise()
    for url in urls:
        ap.add(url)

    fut = _executor.submit(ap.notify, title=title, body=body)
    try:
        result = fut.result(timeout=timeout)
    except _FuturesTimeoutError:
        warnings.warn(
            f"metaflow-notifications: notification did not complete within {timeout}s timeout",
            stacklevel=2,
        )
        return
    except Exception as exc:  # noqa: BLE001
        warnings.warn(
            f"metaflow-notifications: notification raised an exception: {type(exc).__name__}",
            stacklevel=2,
        )
        return

    if result is False:
        warnings.warn(
            "metaflow-notifications: notification dispatch returned False (delivery may have failed)",
            stacklevel=2,
        )


class NotifyStepDecorator(StepDecorator):
    name = "notify"
    defaults = _NOTIFY_DEFAULTS

    def init(self):
        self._run_id = None
        self._task_id = "0"
        self._flow_name = "UnknownFlow"
        self._apprise_available = None

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
            key = (run_id, step_name)
            if key not in _foreach_warned:
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
        # Suppress if Metaflow's @catch already handled it
        if type(exception).__name__ == "FailureHandledByCatch":
            return

        # Only notify on the final failure (no more retries remaining)
        if retry_count < max_user_code_retries:
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
            return [notifier]
        return list(notifier)

    def _fire(self, step_name, run_id, event, spec, *, error=None):
        """Resolve URLs, build message, dispatch notification."""
        if not self._apprise_available:
            return

        urls = self._resolve_urls()
        if not urls:
            return

        msg = _message.resolve_message(
            spec,
            event=event,
            flow_name=self._flow_name,
            run_id=run_id or "?",
            step_name=step_name,
            task_id=self._task_id,
            error=error,
        )
        if msg is None:
            return

        # Build title
        title_spec = self.attributes.get("title", True)
        title = _message.resolve_message(
            title_spec,
            event=event,
            flow_name=self._flow_name,
            run_id=run_id or "?",
            step_name=step_name,
            task_id=self._task_id,
            error=error,
        ) or f"Metaflow: {self._flow_name}"

        timeout = self.attributes.get("timeout", 10)
        _dispatch(urls, title=title, body=msg, timeout=timeout)


class NotifyFlowDecorator(FlowDecorator):
    name = "notify"
    defaults = _NOTIFY_DEFAULTS

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
            step_deco._run_id = None
            step_deco._task_id = "0"
            step_deco._flow_name = flow.__class__.__name__
            step_deco._apprise_available = apprise_available
            step_deco.logger = logger

            node.decorators.append(step_deco)
