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
            try:
                error_repr = repr(error)
                # Force plain str to prevent str-subclass __format__ injection
                # (repr() may return a str subclass whose __format__ runs during f-string interpolation)
                error_repr = str.__new__(str, error_repr)
            except Exception:
                error_repr = f"<{type(error).__name__}: unprintable>"
            body += f"\n{error_repr}"
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

    if isinstance(spec, str):
        # Force a plain str to prevent str-subclass __format__ injection.
        # error.__str__() may return a str subclass whose custom __format__ would execute
        # during spec.format(**ctx), potentially reading attributes from other context values.
        try:
            safe_error = str.__new__(str, str(error)) if error is not None else ""
        except Exception:
            safe_error = f"<{type(error).__name__}: unprintable>" if error is not None else ""
        ctx = dict(
            flow_name=flow_name,
            run_id=run_id,
            step_name=step_name,
            task_id=task_id,
            event=event,
            error=safe_error,
        )
        try:
            return spec.format(**ctx)
        except Exception as exc:
            available = ", ".join(sorted(ctx.keys()))
            warnings.warn(
                f"metaflow-notifications: message template could not be formatted "
                f"({type(exc).__name__}); available variables: {available}",
                stacklevel=2,
            )
            return spec

    if callable(spec):
        # Security note: the live exception object is passed to the callable with its
        # __traceback__ intact. Python tracebacks retain references to all local variables
        # in every stack frame. Callable specs must be trusted code; do not use user-supplied
        # callables with live exceptions in environments where local variable exposure is a concern.
        ctx_with_live_error = dict(
            flow_name=flow_name,
            run_id=run_id,
            step_name=step_name,
            task_id=task_id,
            event=event,
            error=error,
        )
        try:
            result = spec(**ctx_with_live_error)
            if result is None or result is False:
                return None
            return str(result)
        except Exception as exc:  # noqa: BLE001
            warnings.warn(
                f"metaflow-notifications: message callable raised {type(exc).__name__}",
                stacklevel=2,
            )
            return None

    # integers, other unknown types → suppress
    return None
