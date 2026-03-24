"""
Shared test helpers for metaflow-notifications tests.
"""

from concurrent.futures import ThreadPoolExecutor
from unittest.mock import MagicMock

# Import metaflow first so its plugin system fully loads our extension module
# before conftest imports from it directly. Without this, the circular import
# (conftest → notify_decorator → metaflow → plugin loading → notify_decorator partial)
# causes "Cannot locate NotifyStepDecorator".
import metaflow  # noqa: F401
import pytest

import metaflow_extensions.notifications.plugins.notify_decorator as _mod
from metaflow_extensions.notifications.plugins.notify_decorator import (
    NotifyFlowDecorator,
    NotifyStepDecorator,
)


def make_step_decorator(**kwargs):
    """
    Construct a NotifyStepDecorator with the given attribute overrides.
    Bypasses __init__ the same way flow_init injection does.
    """
    defaults = dict(NotifyStepDecorator.defaults)
    defaults.update(kwargs)
    deco = NotifyStepDecorator.__new__(NotifyStepDecorator)
    deco.attributes = defaults
    deco.logger = MagicMock()
    deco._run_id = None
    deco._task_id = "0"
    deco._flow_name = "TestFlow"
    deco._apprise_available = True
    deco._is_foreach_worker = False
    return deco


def make_flow_decorator(**kwargs):
    defaults = dict(NotifyFlowDecorator.defaults)
    defaults.update(kwargs)
    deco = NotifyFlowDecorator.__new__(NotifyFlowDecorator)
    deco.attributes = defaults
    deco.logger = MagicMock()
    return deco


def make_flow(name="TestFlow", **attrs):
    FlowCls = type(name, (), {})
    flow = MagicMock()
    flow.__class__ = FlowCls
    for k, v in attrs.items():
        setattr(flow, k, v)
    return flow


def make_graph_node(name, decorator_names=None):
    """Create a mock graph node with named decorators."""
    node = MagicMock()
    node.name = name
    node.decorators = []
    for dname in decorator_names or []:
        d = MagicMock()
        d.name = dname
        d.attributes = {}
        node.decorators.append(d)
    return node


def make_graph(nodes):
    """nodes is a list of (name, decorator_names) tuples."""
    return [make_graph_node(name, dnames) for name, dnames in nodes]


@pytest.fixture(autouse=True)
def _reset_foreach_warned():
    _mod._foreach_warned.clear()
    yield
    _mod._foreach_warned.clear()


@pytest.fixture(autouse=True)
def _fresh_executor(monkeypatch):
    """Replace the module-level executor with a fresh one per test."""
    fresh = ThreadPoolExecutor(max_workers=4, thread_name_prefix="mf-notify-test")
    monkeypatch.setattr(_mod, "_executor", fresh)
    yield fresh
    fresh.shutdown(wait=True)


@pytest.fixture(autouse=True)
def _reset_notify_urls(monkeypatch):
    """Ensure METAFLOW_NOTIFY_URLS is absent between tests.

    Tests that set this variable directly via os.environ (instead of monkeypatch)
    would otherwise pollute the environment for subsequent tests in the session.
    Using monkeypatch here guarantees the variable is restored to its pre-test
    state regardless of how individual tests modify it.
    """
    monkeypatch.delenv("METAFLOW_NOTIFY_URLS", raising=False)
    yield


@pytest.fixture(autouse=True)
def _reset_apprise_logger_flag(monkeypatch):
    """Reset the module-level apprise logger init flag between tests.

    Without this, the first test that triggers _dispatch configures the apprise
    logger and sets _apprise_logger_configured=True, preventing subsequent tests
    from observing the configuration path.
    """
    monkeypatch.setattr(_mod, "_apprise_logger_configured", False)
