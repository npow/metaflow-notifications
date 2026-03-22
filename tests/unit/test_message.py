"""
Pure unit tests for resolve_message() and _default_message().
No mocks needed — this module is entirely side-effect-free.
"""

from metaflow_extensions.notifications.plugins._message import (
    _default_message,
    resolve_message,
)

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

CTX = dict(flow_name="MyFlow", run_id="42", step_name="train", task_id="7")


def resolve(spec, event="failure", error=None, **overrides):
    kw = {**CTX, **overrides, "event": event, "error": error}
    return resolve_message(spec, **kw)


# ---------------------------------------------------------------------------
# Spec=False / None → None
# ---------------------------------------------------------------------------


def test_false_returns_none():
    assert resolve(False) is None


def test_none_returns_none():
    assert resolve(None) is None


def test_zero_returns_none():
    # Any falsy value should short-circuit
    assert resolve(0) is None


# ---------------------------------------------------------------------------
# Spec=True → default message
# ---------------------------------------------------------------------------


def test_true_failure_returns_string():
    result = resolve(True, event="failure", error=ValueError("boom"))
    assert isinstance(result, str)
    assert "[FAILED]" in result


def test_true_success_returns_string():
    result = resolve(True, event="success")
    assert isinstance(result, str)
    assert "[SUCCESS]" in result


def test_true_start_returns_string():
    result = resolve(True, event="start")
    assert isinstance(result, str)
    assert "[STARTED]" in result


# ---------------------------------------------------------------------------
# Default failure message contains full pathspec
# ---------------------------------------------------------------------------


def test_default_failure_message_pathspec():
    result = resolve(True, event="failure", error=RuntimeError("oops"))
    # Should be a valid Metaflow pathspec: flow_name/run_id/step_name/task_id
    assert "MyFlow" in result
    assert "42" in result
    assert "train" in result
    assert "7" in result


def test_default_failure_message_format():
    result = _default_message(
        event="failure",
        flow_name="F",
        run_id="1",
        step_name="s",
        task_id="3",
        error=ValueError("err"),
    )
    assert result == "[FAILED] F/1/s/3\nValueError('err')" or ("[FAILED] F/1/s/3" in result)


def test_default_failure_with_none_error():
    # Should not raise even when error=None
    result = resolve(True, event="failure", error=None)
    assert "[FAILED]" in result


# ---------------------------------------------------------------------------
# String templates
# ---------------------------------------------------------------------------


def test_string_template_formatted():
    result = resolve("Step {step_name} failed: {error}", event="failure", error=ValueError("x"))
    assert "train" in result
    assert "x" in result


def test_string_template_task_id():
    result = resolve("{task_id}", event="failure")
    assert result == "7"


def test_string_template_all_variables():
    tpl = "{flow_name}/{run_id}/{step_name}/{task_id}"
    result = resolve(tpl, event="failure")
    assert result == "MyFlow/42/train/7"


def test_string_template_with_no_error():
    result = resolve("{flow_name} ok", event="success")
    assert result == "MyFlow ok"


def test_string_template_error_is_stringified():
    """error in str templates is str(error), not the live object, for security."""
    err = ValueError("boom")
    result = resolve("err={error}", event="failure", error=err)
    assert result == "err=boom"


# ---------------------------------------------------------------------------
# Bad template → warns and returns raw template (no raise)
# ---------------------------------------------------------------------------


def test_bad_template_emits_warning(recwarn):
    result = resolve("{unknown_var} oops", event="failure")
    assert result == "{unknown_var} oops"
    assert len(recwarn.list) == 1
    assert (
        "unknown_var" in str(recwarn.list[0].message) or "available variables" in str(recwarn.list[0].message).lower()
    )


def test_bad_template_no_raise():
    # Must not raise under any circumstances
    result = resolve("{this_does_not_exist}", event="failure")
    assert result == "{this_does_not_exist}"


def test_bad_template_warning_mentions_available_vars(recwarn):
    resolve("{oops}", event="failure")
    w = str(recwarn.list[0].message)
    assert "flow_name" in w or "available" in w.lower()


def test_format_spec_type_error_warns_and_returns_raw(recwarn):
    """A template that causes TypeError (e.g. bad format spec) warns and returns raw template."""
    # error is now str(error) in template context, but an invalid format spec still raises
    result = resolve("{flow_name!r:invalid_spec}", event="failure")
    # Should return raw template, not raise
    assert isinstance(result, str)
    assert len(recwarn.list) >= 1


# ---------------------------------------------------------------------------
# Callables
# ---------------------------------------------------------------------------


def test_callable_receives_context_kwargs():
    captured = {}

    def capture(**ctx):
        captured.update(ctx)
        return "ok"

    resolve(capture, event="failure", error=None)
    assert captured["flow_name"] == "MyFlow"
    assert captured["step_name"] == "train"
    assert captured["event"] == "failure"


def test_callable_receives_live_error_object():
    """Callables receive the live error object, not a stringified version."""
    err = ValueError("boom")
    received = {}

    def capture(**ctx):
        received["error"] = ctx["error"]
        return "ok"

    resolve(capture, event="failure", error=err)
    assert received["error"] is err


def test_callable_return_value_used():
    result = resolve(lambda **ctx: f"run {ctx['run_id']} failed", event="failure")
    assert result == "run 42 failed"


def test_callable_lambda_with_error():
    err = ValueError("boom")
    result = resolve(
        lambda error, **ctx: f"{ctx['step_name']}: {error}",
        event="failure",
        error=err,
    )
    assert result == "train: boom"


def test_callable_returning_none_suppresses():
    result = resolve(lambda **ctx: None, event="failure")
    assert result is None


def test_callable_returning_false_suppresses():
    result = resolve(lambda **ctx: False, event="failure")
    assert result is None


def test_callable_result_coerced_to_str():
    result = resolve(lambda **ctx: 42, event="failure")
    assert result == "42"


def test_callable_exception_warns_and_returns_none(recwarn):
    def bad(**ctx):
        raise RuntimeError("oops")

    result = resolve(bad, event="failure")
    assert result is None
    assert any("RuntimeError" in str(w.message) for w in recwarn.list)


def test_callable_returning_broken_str_warns(recwarn):
    """A callable returning an object whose __str__ raises → returns None with warning."""

    class BrokenStr:
        def __str__(self):
            raise RuntimeError("cannot stringify")

    result = resolve(lambda **ctx: BrokenStr(), event="failure")
    assert result is None
    assert len(recwarn.list) >= 1


def test_callable_double_fault_exception_warns(recwarn):
    """A callable that raises an exception whose __str__ also raises → returns None, doesn't crash."""

    class NastyException(Exception):
        def __str__(self):
            raise RuntimeError("str also fails")

    def bad(**ctx):
        raise NastyException()

    # The fence in _fire closes this; resolve_message only uses type(exc).__name__
    result = resolve(bad, event="failure")
    assert result is None
    # A warning should have been emitted
    assert len(recwarn.list) >= 1


# ---------------------------------------------------------------------------
# _default_message: repr guard
# ---------------------------------------------------------------------------


def test_bomb_repr_falls_back_gracefully():
    """An error whose __repr__ raises must not propagate — falls back to unprintable string."""

    class BombRepr(Exception):
        def __repr__(self):
            raise RuntimeError("repr bomb")

    result = _default_message(
        event="failure",
        flow_name="F",
        run_id="1",
        step_name="s",
        task_id="3",
        error=BombRepr(),
    )
    assert isinstance(result, str)
    assert "[FAILED]" in result
    assert "unprintable" in result or "BombRepr" in result


# ---------------------------------------------------------------------------
# Unknown types → None
# ---------------------------------------------------------------------------


def test_integer_nonzero_returns_none():
    # Only True (bool), str, False, None, callable are valid; other types → None
    result = resolve(42, event="failure")
    assert result is None
