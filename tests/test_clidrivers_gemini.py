"""test_clidrivers_gemini.py — GeminiDriver (DM-CLI-04, multi-cli-clients).

Pure-logic tests, no daemon boot. UNLIKE test_clidrivers_codex.py, the
`parse_stream_line` sample lines here are SYNTHESIZED best-guesses (see
clidrivers/gemini.py's module docstring) — this machine has no
GEMINI_API_KEY, so no real `gemini` stdout was ever captured. These
tests pin the driver's OWN documented-best-effort contract, not a
verified real wire format; a live smoke turn (task DM-CLI-04's "Done
when") is still required before trusting this beyond argv-shape.
"""

from __future__ import annotations

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from clidrivers.base import Final, TextDelta, ToolUse  # noqa: E402
from clidrivers.gemini import GeminiDriver  # noqa: E402


def _driver() -> GeminiDriver:
    return GeminiDriver()


# ── build_args ───────────────────────────────────────────────────────


def test_build_args_prompt_goes_in_argv_not_stdin() -> None:
    args = _driver().build_args(
        "/usr/bin/gemini",
        prompt="hello world",
        model=None,
        effort=None,
        session_id="sid",
        use_session=False,
    )
    assert args[0] == "/usr/bin/gemini"
    assert "-p" in args
    assert args[args.index("-p") + 1] == "hello world"
    assert "-o" in args and args[args.index("-o") + 1] == "stream-json"
    assert "-y" in args
    assert "--skip-trust" in args


def test_build_args_model_passthrough() -> None:
    args = _driver().build_args(
        "/usr/bin/gemini",
        prompt="hi",
        model="gemini-2.5-pro",
        effort="high",  # ignored — no effort flag exists on this CLI
        session_id="sid",
        use_session=False,
    )
    assert "-m" in args and args[args.index("-m") + 1] == "gemini-2.5-pro"


def test_efforts_catalog_is_honest_about_no_flag() -> None:
    # gemini --help (0.49.0) has no reasoning-effort flag at all —
    # the catalog must not invent levels that don't exist.
    efforts = _driver().efforts_catalog()
    assert [e["id"] for e in efforts] == ["default"]


# ── write_prompt is a no-op (prompt already in argv) ───────────────────


class _FakeStdin:
    def __init__(self) -> None:
        self.wrote = False
        self.closed = False

    def write(self, _b: bytes) -> None:
        self.wrote = True

    def close(self) -> None:
        self.closed = True


class _FakeProc:
    def __init__(self) -> None:
        self.stdin = _FakeStdin()


def test_write_prompt_closes_stdin_without_writing() -> None:
    proc = _FakeProc()
    _driver().write_prompt(proc, "should not be written")
    assert proc.stdin.wrote is False
    assert proc.stdin.closed is True


# ── parse_stream_line — best-effort shapes ──────────────────────────────


def test_parse_core_gemini_api_shape() -> None:
    line = '{"candidates":[{"content":{"parts":[{"text":"PONG"}]}}]}'
    evs = _driver().parse_stream_line(line)
    assert any(isinstance(e, TextDelta) and e.text == "PONG" for e in evs)


def test_parse_flat_text_field_fallback() -> None:
    evs = _driver().parse_stream_line('{"text":"hello"}')
    assert any(isinstance(e, TextDelta) and e.text == "hello" for e in evs)


def test_parse_done_flag_emits_final() -> None:
    evs = _driver().parse_stream_line('{"text":"the end","done":true}')
    assert any(isinstance(e, Final) for e in evs)


def test_parse_function_call_shape() -> None:
    evs = _driver().parse_stream_line(
        '{"functionCall":{"name":"run_shell","args":{"cmd":"ls"}}}'
    )
    assert any(isinstance(e, ToolUse) and e.name == "run_shell" for e in evs)


def test_parse_malformed_and_non_dict_lines_never_raise() -> None:
    assert _driver().parse_stream_line("not json") == []
    assert _driver().parse_stream_line("[1,2]") == []
    assert _driver().parse_stream_line("") == []


# ── auth/install detection ──────────────────────────────────────────────


def test_auth_configured_true_when_api_key_env_set(monkeypatch) -> None:
    monkeypatch.setenv("GEMINI_API_KEY", "fake-for-test")
    assert _driver().auth_configured() is True


def test_is_transient_error_allowlist() -> None:
    d = _driver()
    assert d.is_transient_error("RESOURCE_EXHAUSTED: quota exceeded") is True
    assert d.is_transient_error("") is False
    assert d.is_transient_error("invalid argument") is False
