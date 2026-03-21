"""
Message resolution for metaflow-notifications.

resolve_message(spec, *, event, flow_name, run_id, step_name, task_id, error) -> str | None

spec values:
  False / None / 0 / falsy non-bool  -> None
  True                               -> _default_message(...)
  str                                -> format with context vars; warn on bad template
  callable(**ctx)                    -> str(result), or None if result is None/False/raises
"""
import warnings


def _default_message(*, event, flow_name, run_id, step_name, task_id, error=None):
    pathspec = f"{flow_name}/{run_id}/{step_name}/{task_id}"
    if event == "failure":
        body = f"[FAILED] {pathspec}"
        if error is not None:
            body += f"\n{repr(error)}"
        return body
    elif event == "success":
        return f"[SUCCESS] {pathspec}"
    elif event == "start":
        return f"[STARTED] {pathspec}"
    else:
        return f"[{event.upper()}] {pathspec}"


def resolve_message(spec, *, event, flow_name, run_id, step_name, task_id, error=None):
    # Falsy non-bool → suppress. True is excluded from the falsy check (bool subclasses int).
    if spec is False or spec is None:
        return None
    if not isinstance(spec, bool) and not spec:
        return None

    if spec is True:
        return _default_message(
            event=event,
            flow_name=flow_name,
            run_id=run_id,
            step_name=step_name,
            task_id=task_id,
            error=error,
        )

    ctx = dict(
        flow_name=flow_name,
        run_id=run_id,
        step_name=step_name,
        task_id=task_id,
        event=event,
        error=error,
    )

    if isinstance(spec, str):
        try:
            return spec.format(**ctx)
        except (KeyError, IndexError) as exc:
            available = ", ".join(sorted(ctx.keys()))
            warnings.warn(
                f"metaflow-notifications: template variable {exc} not found. "
                f"Available variables: {available}",
                stacklevel=2,
            )
            return spec

    if callable(spec):
        try:
            result = spec(**ctx)
        except Exception as exc:  # noqa: BLE001
            warnings.warn(
                f"metaflow-notifications: message callable raised {type(exc).__name__}: {exc}",
                stacklevel=2,
            )
            return None
        if result is None or result is False:
            return None
        return str(result)

    # integers, other unknown types → suppress
    return None
