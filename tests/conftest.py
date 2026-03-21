"""
Shared test helpers for metaflow-notifications tests.
"""
from unittest.mock import MagicMock

# Import metaflow first so its plugin system fully loads our extension module
# before conftest imports from it directly. Without this, the circular import
# (conftest → notify_decorator → metaflow → plugin loading → notify_decorator partial)
# causes "Cannot locate NotifyStepDecorator".
import metaflow  # noqa: F401

from metaflow_extensions.notifications.plugins.notify_decorator import (
    NotifyStepDecorator,
    NotifyFlowDecorator,
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
    return deco


def make_flow_decorator(**kwargs):
    defaults = dict(NotifyFlowDecorator.defaults)
    defaults.update(kwargs)
    deco = NotifyFlowDecorator.__new__(NotifyFlowDecorator)
    deco.attributes = defaults
    deco.logger = MagicMock()
    return deco


def make_flow(name="TestFlow", **attrs):
    flow = MagicMock()
    flow.__class__.__name__ = name
    for k, v in attrs.items():
        setattr(flow, k, v)
    return flow


def make_graph_node(name, decorator_names=None):
    """Create a mock graph node with named decorators."""
    node = MagicMock()
    node.name = name
    node.decorators = []
    for dname in (decorator_names or []):
        d = MagicMock()
        d.name = dname
        d.attributes = {}
        node.decorators.append(d)
    return node


def make_graph(nodes):
    """nodes is a list of (name, decorator_names) tuples."""
    return [make_graph_node(name, dnames) for name, dnames in nodes]
