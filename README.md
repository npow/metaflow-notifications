# metaflow-notifications

[![CI](https://github.com/npow/metaflow-notifications/actions/workflows/ci.yml/badge.svg)](https://github.com/npow/metaflow-notifications/actions/workflows/ci.yml)
[![PyPI](https://img.shields.io/pypi/v/metaflow-notifications)](https://pypi.org/project/metaflow-notifications/)
[![License: Apache-2.0](https://img.shields.io/badge/License-Apache--2.0-blue.svg)](LICENSE)
[![Python 3.8+](https://img.shields.io/badge/python-3.8+-blue.svg)](https://www.python.org/downloads/)
[![Docs](https://img.shields.io/badge/docs-mintlify-18a34a?style=flat-square)](https://mintlify.com/npow/metaflow-notifications)

Add Slack (or any) alerts to your Metaflow pipelines with one decorator.

## Quick start

```bash
pip install metaflow-notifications
```

```python
from metaflow import FlowSpec, step
from metaflow_extensions.notifications import notify

@notify(notifier="slack://T.../B.../XXX")
class TrainingFlow(FlowSpec):

    @step
    def start(self):
        self.next(self.end)

    @step
    def end(self):
        pass
```

Every step in the flow will fire a Slack message on failure. No other changes required.

## Install

```bash
pip install metaflow-notifications
```

Set a URL via environment variable to keep credentials out of source:

```bash
export METAFLOW_NOTIFY_URLS="slack://T.../B.../XXX"
```

## Usage

### Notify on success too

```python
@notify(on_failure=True, on_success=True, notifier="slack://T.../B.../XXX")
class TrainingFlow(FlowSpec):
    ...
```

### Silence a specific step

```python
@notify(notifier="slack://T.../B.../XXX")
class TrainingFlow(FlowSpec):

    @notify(on_failure=False)
    @step
    def noisy_step(self):
        ...
```

### Custom message (string template)

```python
@notify(
    notifier="slack://T.../B.../XXX",
    message="Flow {flow_name} run {run_id} failed at {step_name}: {error}",
)
class TrainingFlow(FlowSpec):
    ...
```

### Custom message (lambda)

Use a callable for full control. It receives the same variables as keyword arguments:

```python
@notify(
    notifier="slack://T.../B.../XXX",
    message=lambda flow_name, step_name, error, **_: (
        f":fire: *{flow_name}/{step_name}* failed\n```{error}```"
    ),
)
class TrainingFlow(FlowSpec):
    ...
```

Return `None` or `False` from the callable to suppress the notification entirely.

**Available variables** (both template and callable):

| Variable | Value |
|----------|-------|
| `flow_name` | Name of the flow class |
| `run_id` | Metaflow run ID |
| `step_name` | Name of the step that fired |
| `task_id` | Task ID within the step |
| `event` | `"failure"`, `"success"`, or `"start"` |
| `error` | The exception object (`None` for non-failure events) |

## Configuration

| Attribute | Default | Description |
|-----------|---------|-------------|
| `on_failure` | `True` | Notify on step failure |
| `on_success` | `False` | Notify on step success |
| `on_start` | `False` | Notify when a step starts |
| `notifier` | `None` | Apprise URL (or `METAFLOW_NOTIFY_URLS`) |
| `timeout` | `10` | Seconds to wait for dispatch |
| `message` | `True` | `True` = default, string = template, `False` = suppress |
| `title` | `True` | `True` = default, string = template, `False` = no title |

Any [Apprise URL](https://github.com/caronc/apprise/wiki) works — Slack, email, PagerDuty, Discord, and 80+ others.

## How it works

A `@notify` flow decorator injects a step decorator on every step that doesn't already have one. Notifications dispatch via [Apprise](https://github.com/caronc/apprise) on a daemon thread with a configurable timeout — they never block your flow. The extension registers via Metaflow's standard extension system and requires no changes to existing flows beyond adding the decorator.

## Development

```bash
git clone https://github.com/npow/metaflow-notifications
cd metaflow-notifications
pip install -e ".[dev]"
pytest -v
```

## License

Apache 2.0 — see [LICENSE](LICENSE).
