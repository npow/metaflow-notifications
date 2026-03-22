"""
Unit tests for NotifyStepDecorator lifecycle hooks.
Apprise is mocked throughout — no real HTTP calls.
"""

import threading
from unittest.mock import MagicMock, patch

from metaflow_extensions.notifications.plugins.notify_decorator import _dispatch
from tests.conftest import make_flow, make_step_decorator

# ---------------------------------------------------------------------------
# task_pre_step — state storage
# ---------------------------------------------------------------------------


def test_task_pre_step_stores_run_id():
    deco = make_step_decorator()
    flow = make_flow()
    deco.task_pre_step("train", None, None, "run42", "1", flow, None, 0, 0, None, None)
    assert deco._run_id == "run42"


def test_task_pre_step_stores_flow_name():
    deco = make_step_decorator()
    flow = make_flow(name="MyFlow")
    deco.task_pre_step("train", None, None, "run42", "1", flow, None, 0, 0, None, None)
    assert deco._flow_name == "MyFlow"


def test_task_pre_step_stores_task_id():
    deco = make_step_decorator()
    flow = make_flow()
    deco.task_pre_step("train", None, None, "run42", "5", flow, None, 0, 0, None, None)
    assert deco._task_id == "5"


def test_task_pre_step_task_id_none_becomes_zero():
    deco = make_step_decorator()
    flow = make_flow()
    deco.task_pre_step("train", None, None, "run42", None, flow, None, 0, 0, None, None)
    assert deco._task_id == "0"


# ---------------------------------------------------------------------------
# on_start: fires only on retry_count == 0
# ---------------------------------------------------------------------------


def test_on_start_fires_on_first_attempt(monkeypatch):
    deco = make_step_decorator(on_start=True, on_failure=False, on_success=False)
    fired = []
    monkeypatch.setattr(deco, "_fire", lambda *a, **kw: fired.append((a, kw)))
    flow = make_flow()
    deco.task_pre_step("train", None, None, "r1", "1", flow, None, 0, 0, None, None)
    assert len(fired) == 1


def test_on_start_suppressed_on_retry(monkeypatch):
    deco = make_step_decorator(on_start=True, on_failure=False, on_success=False)
    fired = []
    monkeypatch.setattr(deco, "_fire", lambda *a, **kw: fired.append((a, kw)))
    flow = make_flow()
    deco.task_pre_step("train", None, None, "r1", "1", flow, None, 1, 2, None, None)
    assert len(fired) == 0


def test_on_start_false_does_not_fire(monkeypatch):
    deco = make_step_decorator(on_start=False)
    fired = []
    monkeypatch.setattr(deco, "_fire", lambda *a, **kw: fired.append((a, kw)))
    flow = make_flow()
    deco.task_pre_step("train", None, None, "r1", "1", flow, None, 0, 0, None, None)
    assert len(fired) == 0


# ---------------------------------------------------------------------------
# on_success
# ---------------------------------------------------------------------------


def test_on_success_fires_when_true(monkeypatch):
    deco = make_step_decorator(on_success=True, on_failure=False)
    fired = []
    monkeypatch.setattr(deco, "_fire", lambda *a, **kw: fired.append((a, kw)))
    deco.task_post_step("train", make_flow(), None, 0, 0)
    assert len(fired) == 1


def test_on_success_suppressed_when_false(monkeypatch):
    deco = make_step_decorator(on_success=False)
    fired = []
    monkeypatch.setattr(deco, "_fire", lambda *a, **kw: fired.append((a, kw)))
    deco.task_post_step("train", make_flow(), None, 0, 0)
    assert len(fired) == 0


def test_task_post_step_uses_stored_run_id(monkeypatch):
    deco = make_step_decorator(on_success=True, on_failure=False)
    calls = []
    monkeypatch.setattr(deco, "_fire", lambda step, run_id, event, spec, **kw: calls.append(run_id))
    deco._run_id = "stored42"
    deco.task_post_step("train", make_flow(), None, 0, 0)
    assert calls == ["stored42"]


def test_on_success_fires_after_retry(monkeypatch):
    """task_post_step fires regardless of retry_count (success = final)."""
    deco = make_step_decorator(on_success=True, on_failure=False)
    deco._is_foreach_worker = False
    fired = []
    monkeypatch.setattr(deco, "_fire", lambda *a, **kw: fired.append((a, kw)))
    deco.task_post_step("train", make_flow(), None, 1, 2)
    assert len(fired) == 1


# ---------------------------------------------------------------------------
# on_failure: retry guards
# ---------------------------------------------------------------------------


def test_on_failure_fires_on_final_retry(monkeypatch):
    deco = make_step_decorator(on_failure=True)
    fired = []
    monkeypatch.setattr(deco, "_fire", lambda *a, **kw: fired.append((a, kw)))
    exc = RuntimeError("fail")
    deco.task_exception(exc, "train", make_flow(), None, 2, 2)
    assert len(fired) == 1


def test_on_failure_suppressed_on_non_final_retry(monkeypatch):
    deco = make_step_decorator(on_failure=True)
    fired = []
    monkeypatch.setattr(deco, "_fire", lambda *a, **kw: fired.append((a, kw)))
    exc = RuntimeError("fail")
    deco.task_exception(exc, "train", make_flow(), None, 0, 2)
    assert len(fired) == 0


def test_on_failure_suppressed_mid_retry(monkeypatch):
    deco = make_step_decorator(on_failure=True)
    fired = []
    monkeypatch.setattr(deco, "_fire", lambda *a, **kw: fired.append((a, kw)))
    exc = RuntimeError("fail")
    deco.task_exception(exc, "train", make_flow(), None, 1, 2)
    assert len(fired) == 0


def test_on_failure_false_does_not_fire(monkeypatch):
    """Verify on_failure=False suppresses; use retry_count=max so retry guard doesn't suppress."""
    deco = make_step_decorator(on_failure=False)
    fired = []
    monkeypatch.setattr(deco, "_fire", lambda *a, **kw: fired.append((a, kw)))
    deco.task_exception(RuntimeError("x"), "train", make_flow(), None, 0, 0)
    assert len(fired) == 0


# ---------------------------------------------------------------------------
# max_user_code_retries=-1 (unlimited retries)
# ---------------------------------------------------------------------------


def test_on_failure_fires_when_unlimited_retries_sentinel(monkeypatch):
    """max_user_code_retries=-1 means unlimited; fire on every exception."""
    deco = make_step_decorator(on_failure=True)
    fired = []
    monkeypatch.setattr(deco, "_fire", lambda *a, **kw: fired.append((a, kw)))
    # retry_count=0 < max would normally suppress, but -1 means unlimited
    deco.task_exception(RuntimeError("x"), "train", make_flow(), None, 0, -1)
    assert len(fired) == 1


# ---------------------------------------------------------------------------
# @catch sentinel suppression
# ---------------------------------------------------------------------------


class _FakeFailureHandledByCatch(Exception):
    pass


_FakeFailureHandledByCatch.__name__ = "FailureHandledByCatch"


def test_catch_sentinel_suppresses_failure(monkeypatch):
    deco = make_step_decorator(on_failure=True)
    fired = []
    monkeypatch.setattr(deco, "_fire", lambda *a, **kw: fired.append((a, kw)))
    sentinel = _FakeFailureHandledByCatch()
    deco.task_exception(sentinel, "train", make_flow(), None, 2, 2)
    assert len(fired) == 0


def test_normal_exception_not_suppressed(monkeypatch):
    deco = make_step_decorator(on_failure=True)
    fired = []
    monkeypatch.setattr(deco, "_fire", lambda *a, **kw: fired.append((a, kw)))
    deco.task_exception(RuntimeError("real"), "train", make_flow(), None, 0, 0)
    assert len(fired) == 1


# ---------------------------------------------------------------------------
# Foreach worker guard
# ---------------------------------------------------------------------------


def test_foreach_worker_task_post_step_does_not_fire(monkeypatch):
    """task_post_step must not fire when _is_foreach_worker is True."""
    deco = make_step_decorator(on_success=True)
    fired = []
    monkeypatch.setattr(deco, "_fire", lambda *a, **kw: fired.append((a, kw)))
    flow = make_flow()
    # Set foreach worker flag via task_pre_step
    deco.task_pre_step("train", None, None, "r1", "1", flow, None, 0, 0, "foreach", None)
    deco.task_post_step("train", flow, None, 0, 0)
    assert len(fired) == 0


def test_foreach_worker_task_exception_does_not_fire(monkeypatch):
    """task_exception must not fire when _is_foreach_worker is True."""
    deco = make_step_decorator(on_failure=True)
    fired = []
    monkeypatch.setattr(deco, "_fire", lambda *a, **kw: fired.append((a, kw)))
    flow = make_flow()
    deco.task_pre_step("train", None, None, "r1", "1", flow, None, 0, 0, "foreach", None)
    deco.task_exception(RuntimeError("x"), "train", flow, None, 0, 0)
    assert len(fired) == 0


# ---------------------------------------------------------------------------
# URL resolution and "no URLs" warning
# ---------------------------------------------------------------------------


def test_no_urls_emits_warning(recwarn, monkeypatch):
    deco = make_step_decorator(notifier=None, on_failure=True)
    # Ensure env var is absent
    monkeypatch.delenv("METAFLOW_NOTIFY_URLS", raising=False)
    deco._run_id = "r1"
    # Patch _dispatch to avoid real calls
    with patch("metaflow_extensions.notifications.plugins.notify_decorator._dispatch"):
        deco._fire("train", "r1", "failure", True, error=RuntimeError("x"))
    assert any("no notifier" in str(w.message).lower() or "no url" in str(w.message).lower() for w in recwarn.list)


def test_env_var_used_when_notifier_none(monkeypatch):
    deco = make_step_decorator(notifier=None, on_failure=True)
    monkeypatch.setenv("METAFLOW_NOTIFY_URLS", "slack://T/C")
    deco._run_id = "r1"
    dispatch_calls = []
    with patch(
        "metaflow_extensions.notifications.plugins.notify_decorator._dispatch",
        side_effect=lambda urls, **kw: dispatch_calls.append(urls),
    ):
        deco._fire("train", "r1", "failure", True, error=RuntimeError("x"))
    assert dispatch_calls
    assert "slack://T/C" in dispatch_calls[0]


def test_notifier_string_used_directly(monkeypatch):
    deco = make_step_decorator(notifier="discord://hook", on_failure=True)
    monkeypatch.delenv("METAFLOW_NOTIFY_URLS", raising=False)
    deco._run_id = "r1"
    dispatch_calls = []
    with patch(
        "metaflow_extensions.notifications.plugins.notify_decorator._dispatch",
        side_effect=lambda urls, **kw: dispatch_calls.append(urls),
    ):
        deco._fire("train", "r1", "failure", True, error=RuntimeError("x"))
    assert dispatch_calls[0] == ["discord://hook"]


def test_notifier_list_used_directly(monkeypatch):
    deco = make_step_decorator(notifier=["slack://a", "pd://b"], on_failure=True)
    monkeypatch.delenv("METAFLOW_NOTIFY_URLS", raising=False)
    deco._run_id = "r1"
    dispatch_calls = []
    with patch(
        "metaflow_extensions.notifications.plugins.notify_decorator._dispatch",
        side_effect=lambda urls, **kw: dispatch_calls.append(urls),
    ):
        deco._fire("train", "r1", "failure", True, error=RuntimeError("x"))
    assert dispatch_calls[0] == ["slack://a", "pd://b"]


def test_blocked_url_scheme_emits_warning(recwarn, monkeypatch):
    """Dangerous URL schemes are blocked with a warning."""
    deco = make_step_decorator(notifier="file:///etc/passwd", on_failure=True)
    monkeypatch.delenv("METAFLOW_NOTIFY_URLS", raising=False)
    deco._run_id = "r1"
    with patch("metaflow_extensions.notifications.plugins.notify_decorator._dispatch") as mock_dispatch:
        deco._fire("train", "r1", "failure", True, error=RuntimeError("x"))
    # _dispatch should not be called since URL was blocked
    mock_dispatch.assert_not_called()
    assert any("blocked" in str(w.message).lower() or "scheme" in str(w.message).lower() for w in recwarn.list)


# ---------------------------------------------------------------------------
# _dispatch — non-fatal, thread-based
# ---------------------------------------------------------------------------


def test_dispatch_non_fatal_on_exception():
    """_dispatch must never raise even if Apprise throws."""
    with patch("apprise.Apprise") as MockApprise:
        MockApprise.return_value.notify.side_effect = RuntimeError("network down")
        # Should not raise
        _dispatch(["slack://x"], title="T", body="B", timeout=5)


def test_dispatch_warning_on_exception(recwarn):
    with patch("apprise.Apprise") as MockApprise:
        MockApprise.return_value.notify.side_effect = OSError("conn refused")
        _dispatch(["slack://x"], title="T", body="B", timeout=5)
    msgs = [str(w.message) for w in recwarn.list]
    assert any("OSError" in m for m in msgs)


def test_dispatch_warning_does_not_include_url(recwarn):
    """Credential URLs must not appear in warning messages."""
    with patch("apprise.Apprise") as MockApprise:
        MockApprise.return_value.notify.side_effect = ConnectionError("slack://secret-token/channel")
        _dispatch(["slack://secret-token/channel"], title="T", body="B", timeout=5)
    msgs = " ".join(str(w.message) for w in recwarn.list)
    assert "secret-token" not in msgs


def test_dispatch_warns_on_false_return(recwarn):
    with patch("apprise.Apprise") as MockApprise:
        MockApprise.return_value.notify.return_value = False
        _dispatch(["slack://x"], title="T", body="B", timeout=5)
    msgs = [str(w.message) for w in recwarn.list]
    assert any("false" in m.lower() or "delivery" in m.lower() for m in msgs)


def test_dispatch_timeout_emits_warning(recwarn, _fresh_executor):
    """When ap.notify() hangs longer than timeout, warn and continue."""
    # Use an event to avoid sleeping in the thread forever
    unblock = threading.Event()

    def slow_notify(*a, **kw):
        unblock.wait(timeout=30)
        return True

    with patch("apprise.Apprise") as MockApprise:
        MockApprise.return_value.notify.side_effect = slow_notify
        # Very short timeout — thread won't finish in time
        _dispatch(["slack://x"], title="T", body="B", timeout=0.05)

    # Unblock the background thread so the executor can shut down cleanly
    unblock.set()

    msgs = [str(w.message) for w in recwarn.list]
    assert any("timeout" in m.lower() or "did not complete" in m.lower() for m in msgs)


def test_dispatch_no_timeout_warning_on_fast_return(recwarn):
    with patch("apprise.Apprise") as MockApprise:
        MockApprise.return_value.notify.return_value = True
        _dispatch(["slack://x"], title="T", body="B", timeout=10)
    msgs = [str(w.message) for w in recwarn.list]
    assert not any("timeout" in m.lower() for m in msgs)


# ---------------------------------------------------------------------------
# step_init — apprise availability check
# ---------------------------------------------------------------------------


def test_step_init_warns_when_apprise_missing(recwarn, monkeypatch):
    deco = make_step_decorator()
    deco._apprise_available = None

    # Simulate apprise not importable
    import builtins

    real_import = builtins.__import__

    def mock_import(name, *args, **kwargs):
        if name == "apprise":
            raise ImportError("No module named 'apprise'")
        return real_import(name, *args, **kwargs)

    monkeypatch.setattr(builtins, "__import__", mock_import)
    deco.step_init(make_flow(), None, "train", [], None, None, MagicMock())
    assert deco._apprise_available is False
    assert any("apprise" in str(w.message).lower() for w in recwarn.list)


def test_step_init_sets_available_when_importable():
    deco = make_step_decorator()
    deco._apprise_available = None
    deco.step_init(make_flow(), None, "train", [], None, None, MagicMock())
    assert deco._apprise_available is True


# ---------------------------------------------------------------------------
# Foreach warn-once
# ---------------------------------------------------------------------------


def test_foreach_warning_emitted_for_foreach_task(recwarn, monkeypatch):
    """When ubf_context is not None and notifications are active, warn once."""
    deco = make_step_decorator(on_failure=True)
    monkeypatch.setattr(deco, "_fire", lambda *a, **kw: None)
    flow = make_flow()
    # ubf_context != None signals foreach context
    deco.task_pre_step("train", None, None, "r1", "1", flow, None, 0, 0, "foreach", None)

    msgs = [str(w.message) for w in recwarn.list]
    assert any("foreach" in m.lower() or "n notification" in m.lower() or "N notification" in m for m in msgs)


def test_foreach_warning_emitted_only_once(recwarn, monkeypatch):
    deco = make_step_decorator(on_failure=True)
    monkeypatch.setattr(deco, "_fire", lambda *a, **kw: None)
    flow = make_flow()

    # Two foreach tasks for same (run_id, step_name)
    deco.task_pre_step("train", None, None, "r1", "1", flow, None, 0, 0, "foreach", None)
    deco2 = make_step_decorator(on_failure=True)
    monkeypatch.setattr(deco2, "_fire", lambda *a, **kw: None)
    deco2.task_pre_step("train", None, None, "r1", "2", flow, None, 0, 0, "foreach", None)

    foreach_warns = [
        w for w in recwarn.list if "foreach" in str(w.message).lower() or "n notification" in str(w.message).lower()
    ]
    assert len(foreach_warns) == 1


def test_no_foreach_warning_for_non_foreach_step(recwarn, monkeypatch):
    deco = make_step_decorator(on_failure=True)
    monkeypatch.setattr(deco, "_fire", lambda *a, **kw: None)
    flow = make_flow()
    # ubf_context=None means normal (non-foreach) step
    deco.task_pre_step("train", None, None, "r1", "1", flow, None, 0, 0, None, None)

    foreach_warns = [w for w in recwarn.list if "foreach" in str(w.message).lower()]
    assert len(foreach_warns) == 0


def test_foreach_warning_different_run_ids_warns_again(recwarn, monkeypatch):
    """Different (run_id, step_name) pairs should each get one warning."""
    flow = make_flow()
    for run_id in ["r1", "r2"]:
        deco = make_step_decorator(on_failure=True)
        monkeypatch.setattr(deco, "_fire", lambda *a, **kw: None)
        deco.task_pre_step("train", None, None, run_id, "1", flow, None, 0, 0, "foreach", None)

    foreach_warns = [
        w for w in recwarn.list if "foreach" in str(w.message).lower() or "n notification" in str(w.message).lower()
    ]
    assert len(foreach_warns) == 2


# ---------------------------------------------------------------------------
# task_id threading through _fire
# ---------------------------------------------------------------------------


def test_task_id_stored_in_task_pre_step():
    deco = make_step_decorator()
    flow = make_flow()
    deco.task_pre_step("train", None, None, "r1", "99", flow, None, 0, 0, None, None)
    assert deco._task_id == "99"


def test_task_id_passed_to_resolve_message(monkeypatch):
    """_fire must pass task_id into resolve_message."""
    deco = make_step_decorator(on_failure=True, notifier="dummy://x")
    monkeypatch.delenv("METAFLOW_NOTIFY_URLS", raising=False)

    resolve_calls = []

    import metaflow_extensions.notifications.plugins._message as msg_mod

    original = msg_mod.resolve_message

    def capturing_resolve(spec, **kwargs):
        resolve_calls.append(kwargs)
        return original(spec, **kwargs)

    monkeypatch.setattr(msg_mod, "resolve_message", capturing_resolve)

    with patch("metaflow_extensions.notifications.plugins.notify_decorator._dispatch"):
        deco._task_id = "77"
        deco._run_id = "r1"
        deco._flow_name = "F"
        deco._fire("train", "r1", "failure", True, error=RuntimeError("x"))

    assert resolve_calls
    assert resolve_calls[0]["task_id"] == "77"


# ---------------------------------------------------------------------------
# _fire: title fallback and None run_id
# ---------------------------------------------------------------------------


def test_fire_title_false_uses_fallback(monkeypatch):
    """When title resolves to falsy, fallback to 'Metaflow: <FlowName>'."""
    deco = make_step_decorator(on_failure=True, notifier="slack://x", title=False)
    monkeypatch.delenv("METAFLOW_NOTIFY_URLS", raising=False)
    deco._run_id = "r1"
    deco._flow_name = "TestFlow"

    dispatch_calls = []
    with patch(
        "metaflow_extensions.notifications.plugins.notify_decorator._dispatch",
        side_effect=lambda urls, **kw: dispatch_calls.append(kw),
    ):
        deco._fire("train", "r1", "failure", True, error=RuntimeError("x"))

    assert dispatch_calls
    assert dispatch_calls[0]["title"] == "Metaflow: TestFlow"


def test_fire_with_none_run_id_uses_question_mark(monkeypatch):
    """When _run_id is None, _fire uses '?' as the run_id in the message."""
    deco = make_step_decorator(on_failure=True, notifier="slack://x")
    monkeypatch.delenv("METAFLOW_NOTIFY_URLS", raising=False)
    deco._run_id = None

    dispatch_calls = []
    with patch(
        "metaflow_extensions.notifications.plugins.notify_decorator._dispatch",
        side_effect=lambda urls, **kw: dispatch_calls.append(kw),
    ):
        deco.task_exception(RuntimeError("boom"), "train", make_flow(), None, 0, 0)

    assert dispatch_calls
    assert "?" in dispatch_calls[0]["body"]


# ---------------------------------------------------------------------------
# Injected decorator safe initial state
# ---------------------------------------------------------------------------


def test_injected_decorator_has_safe_run_id():
    from metaflow_extensions.notifications.plugins.notify_decorator import (
        NotifyStepDecorator,
    )

    deco = NotifyStepDecorator.__new__(NotifyStepDecorator)
    deco.attributes = dict(NotifyStepDecorator.defaults)
    deco.logger = MagicMock()
    deco.init()
    assert deco._run_id is None


def test_injected_decorator_has_safe_task_id():
    from metaflow_extensions.notifications.plugins.notify_decorator import (
        NotifyStepDecorator,
    )

    deco = NotifyStepDecorator.__new__(NotifyStepDecorator)
    deco.attributes = dict(NotifyStepDecorator.defaults)
    deco.logger = MagicMock()
    deco.init()
    assert deco._task_id == "0"


def test_init_is_idempotent():
    """Calling init() twice must not reset state set between calls."""
    from metaflow_extensions.notifications.plugins.notify_decorator import (
        NotifyStepDecorator,
    )

    deco = NotifyStepDecorator.__new__(NotifyStepDecorator)
    deco.attributes = dict(NotifyStepDecorator.defaults)
    deco.logger = MagicMock()
    deco.init()
    deco._run_id = "already_set"
    deco.init()  # second call should be a no-op
    assert deco._run_id == "already_set"
