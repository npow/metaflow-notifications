"""
Unit tests for NotifyFlowDecorator.flow_init injection logic.
"""

from unittest.mock import MagicMock, patch

from tests.conftest import make_flow, make_flow_decorator, make_graph

# ---------------------------------------------------------------------------
# flow_init — step injection
# ---------------------------------------------------------------------------


def test_flow_init_injects_decorator_on_all_steps():
    """Steps without @notify get a NotifyStepDecorator injected."""
    deco = make_flow_decorator()
    flow = make_flow()
    graph = make_graph([("start", []), ("end", [])])
    env = MagicMock()
    datastore = MagicMock()
    metadata = MagicMock()
    logger = MagicMock()
    echo = MagicMock()
    options = {}

    with patch("metaflow_extensions.notifications.plugins.notify_decorator.NotifyStepDecorator") as MockStep:
        deco.flow_init(flow, graph, env, datastore, metadata, logger, echo, options)

    # One injected decorator per step
    assert MockStep.call_count == 2


def test_flow_init_skips_step_with_existing_notify():
    """Steps that already have a @notify decorator are not double-decorated."""
    deco = make_flow_decorator()
    flow = make_flow()
    graph = make_graph([("start", ["notify"]), ("end", [])])
    env = MagicMock()
    datastore = MagicMock()
    metadata = MagicMock()
    logger = MagicMock()
    echo = MagicMock()
    options = {}

    with patch("metaflow_extensions.notifications.plugins.notify_decorator.NotifyStepDecorator") as MockStep:
        deco.flow_init(flow, graph, env, datastore, metadata, logger, echo, options)

    # Only "end" step gets an injected decorator, not "start"
    assert MockStep.call_count == 1


def test_flow_init_all_steps_have_notify_no_injection():
    """When all steps already have @notify, no injection occurs."""
    deco = make_flow_decorator()
    flow = make_flow()
    graph = make_graph([("start", ["notify"]), ("end", ["notify"])])
    env = MagicMock()
    datastore = MagicMock()
    metadata = MagicMock()
    logger = MagicMock()
    echo = MagicMock()
    options = {}

    with patch("metaflow_extensions.notifications.plugins.notify_decorator.NotifyStepDecorator") as MockStep:
        deco.flow_init(flow, graph, env, datastore, metadata, logger, echo, options)

    assert MockStep.call_count == 0


def test_flow_init_injected_decorator_has_flow_defaults():
    """Injected step decorator inherits flow-level attribute values."""
    from metaflow_extensions.notifications.plugins.notify_decorator import NotifyFlowDecorator

    deco = NotifyFlowDecorator.__new__(NotifyFlowDecorator)
    deco.attributes = dict(NotifyFlowDecorator.defaults)
    deco.attributes.update({"on_failure": True, "on_success": True, "notifier": "slack://x"})
    deco.logger = MagicMock()

    flow = make_flow()
    graph = make_graph([("start", [])])
    node = graph[0]
    env = MagicMock()

    deco.flow_init(flow, graph, env, MagicMock(), MagicMock(), MagicMock(), MagicMock(), {})

    assert len(node.decorators) == 1
    injected = node.decorators[0]
    assert injected.attributes["on_failure"] is True
    assert injected.attributes["on_success"] is True
    assert injected.attributes["notifier"] == "slack://x"


def test_flow_init_injected_decorator_appended_to_node_decorators():
    """Injected decorator is appended to node.decorators list."""
    deco = make_flow_decorator()
    flow = make_flow()
    graph = make_graph([("start", [])])
    node = graph[0]
    env = MagicMock()
    datastore = MagicMock()
    metadata = MagicMock()
    logger = MagicMock()
    echo = MagicMock()
    options = {}

    initial_count = len(node.decorators)

    with patch("metaflow_extensions.notifications.plugins.notify_decorator.NotifyStepDecorator") as MockStep:
        MockStep.return_value = MagicMock()
        deco.flow_init(flow, graph, env, datastore, metadata, logger, echo, options)

    assert len(node.decorators) == initial_count + 1


# ---------------------------------------------------------------------------
# flow_init — conflict warnings
# ---------------------------------------------------------------------------


def test_flow_init_warns_on_failure_false_conflict(recwarn):
    """Step with on_failure=False when flow has on_failure=True → warning."""
    deco = make_flow_decorator(on_failure=True)
    flow = make_flow()
    # Step has explicit notify with on_failure=False
    graph = make_graph([("start", ["notify"])])
    graph[0].decorators[0].attributes = {"on_failure": False}
    env = MagicMock()
    datastore = MagicMock()
    metadata = MagicMock()
    logger = MagicMock()
    echo = MagicMock()
    options = {}

    with patch("metaflow_extensions.notifications.plugins.notify_decorator.NotifyStepDecorator"):
        deco.flow_init(flow, graph, env, datastore, metadata, logger, echo, options)

    msgs = [str(w.message) for w in recwarn.list]
    assert any("on_failure" in m.lower() or "conflict" in m.lower() or "suppress" in m.lower() for m in msgs)


def test_flow_init_warns_kubernetes_remote_execution(recwarn):
    """Step with @kubernetes decorator → warning about remote execution."""
    deco = make_flow_decorator(notifier="slack://x")
    flow = make_flow()
    graph = make_graph([("start", ["kubernetes"])])
    env = MagicMock()
    datastore = MagicMock()
    metadata = MagicMock()
    logger = MagicMock()
    echo = MagicMock()
    options = {}

    with patch("metaflow_extensions.notifications.plugins.notify_decorator.NotifyStepDecorator"):
        deco.flow_init(flow, graph, env, datastore, metadata, logger, echo, options)

    msgs = [str(w.message) for w in recwarn.list]
    assert any("kubernetes" in m.lower() or "remote" in m.lower() or "worker" in m.lower() for m in msgs)


def test_flow_init_warns_batch_remote_execution(recwarn):
    """Step with @batch decorator → warning about remote execution."""
    deco = make_flow_decorator(notifier="slack://x")
    flow = make_flow()
    graph = make_graph([("start", ["batch"])])
    env = MagicMock()
    datastore = MagicMock()
    metadata = MagicMock()
    logger = MagicMock()
    echo = MagicMock()
    options = {}

    with patch("metaflow_extensions.notifications.plugins.notify_decorator.NotifyStepDecorator"):
        deco.flow_init(flow, graph, env, datastore, metadata, logger, echo, options)

    msgs = [str(w.message) for w in recwarn.list]
    assert any("batch" in m.lower() or "remote" in m.lower() or "worker" in m.lower() for m in msgs)


def test_flow_init_no_remote_warning_without_url(recwarn):
    """Remote execution warning only fires when a URL is configured."""
    deco = make_flow_decorator(notifier=None)
    flow = make_flow()
    graph = make_graph([("start", ["kubernetes"])])
    env = MagicMock()
    datastore = MagicMock()
    metadata = MagicMock()
    logger = MagicMock()
    echo = MagicMock()
    options = {}

    with patch("metaflow_extensions.notifications.plugins.notify_decorator.NotifyStepDecorator"):
        deco.flow_init(flow, graph, env, datastore, metadata, logger, echo, options)

    msgs = [str(w.message) for w in recwarn.list]
    # No URL → no point warning about remote (notifications won't fire anyway)
    assert not any("kubernetes" in m.lower() or "remote" in m.lower() for m in msgs)


# ---------------------------------------------------------------------------
# flow_init — injected decorator initial state
# ---------------------------------------------------------------------------


def test_flow_init_injected_decorator_run_id_is_none():
    """Injected decorator starts with _run_id = None (set later in task_pre_step)."""
    deco = make_flow_decorator()
    flow = make_flow()
    graph = make_graph([("start", [])])
    node = graph[0]
    env = MagicMock()

    deco.flow_init(flow, graph, env, MagicMock(), MagicMock(), MagicMock(), MagicMock(), {})

    assert len(node.decorators) == 1
    injected = node.decorators[0]
    assert injected._run_id is None
    assert injected._task_id == "0"


# ---------------------------------------------------------------------------
# Integration: injected notifier reaches _dispatch
# ---------------------------------------------------------------------------


def test_flow_injected_notifier_reaches_dispatch(monkeypatch):
    """End-to-end: flow-level notifier URL is passed to _dispatch on task_exception."""
    from metaflow_extensions.notifications.plugins.notify_decorator import NotifyFlowDecorator

    deco = NotifyFlowDecorator.__new__(NotifyFlowDecorator)
    deco.attributes = dict(NotifyFlowDecorator.defaults)
    deco.attributes["notifier"] = "slack://x"
    deco.logger = MagicMock()

    flow = make_flow()
    graph = make_graph([("start", [])])
    node = graph[0]

    deco.flow_init(flow, graph, MagicMock(), MagicMock(), MagicMock(), MagicMock(), MagicMock(), {})

    injected = node.decorators[0]
    monkeypatch.delenv("METAFLOW_NOTIFY_URLS", raising=False)

    dispatch_calls = []
    with patch(
        "metaflow_extensions.notifications.plugins.notify_decorator._dispatch",
        side_effect=lambda urls, **kw: dispatch_calls.append(urls),
    ):
        injected.task_exception(RuntimeError("boom"), "start", flow, None, 0, 0)

    assert dispatch_calls
    assert dispatch_calls[0] == ["slack://x"]


def test_message_attribute_is_not_used_as_body_spec(monkeypatch):
    """message= attribute is not a body override — on_failure=True drives the body.

    NOTE: This is intentional. The `message` attribute is reserved for future use
    or alternate semantics. The body spec is always driven by on_failure/on_success/on_start.
    """
    from metaflow_extensions.notifications.plugins.notify_decorator import NotifyFlowDecorator

    deco = NotifyFlowDecorator.__new__(NotifyFlowDecorator)
    deco.attributes = dict(NotifyFlowDecorator.defaults)
    deco.attributes["notifier"] = "slack://x"
    deco.attributes["message"] = "override"
    deco.logger = MagicMock()

    flow = make_flow()
    graph = make_graph([("start", [])])
    node = graph[0]

    deco.flow_init(flow, graph, MagicMock(), MagicMock(), MagicMock(), MagicMock(), MagicMock(), {})

    injected = node.decorators[0]
    monkeypatch.delenv("METAFLOW_NOTIFY_URLS", raising=False)

    dispatch_calls = []
    with patch(
        "metaflow_extensions.notifications.plugins.notify_decorator._dispatch",
        side_effect=lambda urls, **kw: dispatch_calls.append(kw),
    ):
        injected.task_exception(RuntimeError("boom"), "start", flow, None, 0, 0)

    assert dispatch_calls
    # Body is the default failure message, not "override"
    assert "override" not in dispatch_calls[0]["body"]
    assert "[FAILED]" in dispatch_calls[0]["body"]


# ---------------------------------------------------------------------------
# NotifyFlowDecorator.defaults
# ---------------------------------------------------------------------------


def test_flow_decorator_defaults():
    """Flow decorator exposes expected default attributes."""
    from metaflow_extensions.notifications.plugins.notify_decorator import (
        NotifyFlowDecorator,
    )

    assert "on_failure" in NotifyFlowDecorator.defaults
    assert "on_success" in NotifyFlowDecorator.defaults
    assert "notifier" in NotifyFlowDecorator.defaults
    assert "timeout" in NotifyFlowDecorator.defaults


def test_flow_decorator_on_failure_default_true():
    """on_failure defaults to True at flow level."""
    from metaflow_extensions.notifications.plugins.notify_decorator import (
        NotifyFlowDecorator,
    )

    assert NotifyFlowDecorator.defaults["on_failure"] is True


def test_flow_decorator_on_success_default_false():
    """on_success defaults to False at flow level."""
    from metaflow_extensions.notifications.plugins.notify_decorator import (
        NotifyFlowDecorator,
    )

    assert NotifyFlowDecorator.defaults["on_success"] is False


def test_step_and_flow_defaults_are_independent():
    """Step and Flow decorators have separate dict copies (mutating one doesn't affect the other)."""
    from metaflow_extensions.notifications.plugins.notify_decorator import (
        NotifyFlowDecorator,
        NotifyStepDecorator,
    )

    try:
        NotifyStepDecorator.defaults["_test_sentinel"] = True
        assert "_test_sentinel" not in NotifyFlowDecorator.defaults
    finally:
        # Use pop() with default to avoid KeyError if assertion failed before the key was set
        NotifyStepDecorator.defaults.pop("_test_sentinel", None)
