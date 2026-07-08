"""test_clidrivers_codex.py — CodexDriver (DM-CLI-05, multi-cli-clients).

Pure-logic tests, no daemon boot — mirrors test_teamext_reap.py's style.
Sample lines below are the ACTUAL NDJSON captured from a real
`codex exec --json` run (codex-cli 0.137.0) during the DM-CLI-03 spike,
not synthesized guesses.
"""

from __future__ import annotations

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from clidrivers.base import Final, TextDelta, ToolResult, ToolUse  # noqa: E402
from clidrivers.codex import CodexDriver  # noqa: E402


def _driver() -> CodexDriver:
    return CodexDriver()


# ── build_args ───────────────────────────────────────────────────────


def test_build_args_base_shape() -> None:
    args = _driver().build_args(
        "/usr/bin/codex",
        prompt="hello",
        model=None,
        effort=None,
        session_id="sid",
        use_session=False,
    )
    assert args[0] == "/usr/bin/codex"
    assert "exec" in args
    assert "--json" in args
    assert "--dangerously-bypass-approvals-and-sandbox" in args
    assert "--skip-git-repo-check" in args
    assert args[-1] == "-"  # prompt via stdin, not argv
    assert "-m" not in args


def test_build_args_model_and_effort() -> None:
    args = _driver().build_args(
        "/usr/bin/codex",
        prompt="hello",
        model="gpt-5.4",
        effort="high",
        session_id="sid",
        use_session=False,
    )
    assert "-m" in args and args[args.index("-m") + 1] == "gpt-5.4"
    assert "-c" in args
    assert 'model_reasoning_effort="high"' in args


def test_build_args_effort_default_omits_flag() -> None:
    args = _driver().build_args(
        "/usr/bin/codex",
        prompt="hello",
        model=None,
        effort="default",
        session_id="sid",
        use_session=False,
    )
    assert "-c" not in args


def test_build_args_max_effort_maps_to_xhigh() -> None:
    args = _driver().build_args(
        "/usr/bin/codex",
        prompt="hello",
        model=None,
        effort="max",
        session_id="sid",
        use_session=False,
    )
    assert 'model_reasoning_effort="xhigh"' in args


# ── parse_stream_line — real captured lines (DM-CLI-03 spike) ──────────


def test_parse_agent_message() -> None:
    line = (
        '{"type":"item.completed","item":{"id":"item_0","type":"agent_message",'
        '"text":"PONG"}}'
    )
    evs = _driver().parse_stream_line(line)
    assert len(evs) == 1
    assert isinstance(evs[0], TextDelta)
    assert evs[0].text == "PONG"


def test_parse_command_execution_start_and_complete() -> None:
    start = (
        '{"type":"item.started","item":{"id":"item_0","type":"command_execution",'
        '"command":"/bin/zsh -lc \'echo hi\'","aggregated_output":"","exit_code":null,'
        '"status":"in_progress"}}'
    )
    done = (
        '{"type":"item.completed","item":{"id":"item_0","type":"command_execution",'
        '"command":"/bin/zsh -lc \'echo hi\'","aggregated_output":"hi\\n",'
        '"exit_code":0,"status":"completed"}}'
    )
    evs_start = _driver().parse_stream_line(start)
    assert len(evs_start) == 1
    assert isinstance(evs_start[0], ToolUse)
    assert evs_start[0].name == "shell"

    evs_done = _driver().parse_stream_line(done)
    assert len(evs_done) == 1
    assert isinstance(evs_done[0], ToolResult)
    assert evs_done[0].ok is True


def test_parse_command_execution_nonzero_exit_is_not_ok() -> None:
    done = (
        '{"type":"item.completed","item":{"id":"item_0","type":"command_execution",'
        '"exit_code":1,"status":"completed"}}'
    )
    evs = _driver().parse_stream_line(done)
    assert evs == [ToolResult(ok=False)]


def test_parse_turn_completed_carries_usage() -> None:
    line = (
        '{"type":"turn.completed","usage":{"input_tokens":25196,'
        '"cached_input_tokens":2432,"output_tokens":6,"reasoning_output_tokens":0}}'
    )
    evs = _driver().parse_stream_line(line)
    assert len(evs) == 1
    assert isinstance(evs[0], Final)
    assert evs[0].usage == {
        "input_tokens": 25196,
        "output_tokens": 6,
        "cache_read_input_tokens": 2432,
        "cache_creation_input_tokens": 0,
    }


def test_parse_error_and_turn_failed_surface_as_final_text() -> None:
    # Real captured output (2026-07-08 live smoke test): an invalid `-m`
    # for the account comes back as `error` then `turn.failed`, NOT as an
    # agent_message. Before this was handled, parse_stream_line returned
    # [] for both lines and the turn silently finalised with EMPTY text
    # and exit=1 — no diagnostic anywhere the operator could see.
    error_line = (
        '{"type":"error","message":"{\\"type\\":\\"error\\",\\"status\\":400,'
        '\\"error\\":{\\"type\\":\\"invalid_request_error\\",\\"message\\":'
        "\\\"The 'opus' model is not supported when using Codex with a "
        'ChatGPT account.\\"}}"}'
    )
    failed_line = (
        '{"type":"turn.failed","error":{"message":"{\\"type\\":\\"error\\",'
        '\\"status\\":400,\\"error\\":{\\"type\\":\\"invalid_request_error\\",'
        '\\"message\\":\\"The \'opus\' model is not supported when using '
        'Codex with a ChatGPT account.\\"}}"}}'
    )
    for line in (error_line, failed_line):
        evs = _driver().parse_stream_line(line)
        assert len(evs) == 1
        assert isinstance(evs[0], Final)
        assert "not supported" in evs[0].text
        assert evs[0].text.startswith("[codex error]")


def test_parse_thread_started_and_turn_started_are_inert() -> None:
    assert (
        _driver().parse_stream_line('{"type":"thread.started","thread_id":"x"}') == []
    )
    assert _driver().parse_stream_line('{"type":"turn.started"}') == []


def test_parse_malformed_and_non_dict_lines_never_raise() -> None:
    assert _driver().parse_stream_line("not json at all") == []
    assert _driver().parse_stream_line("[1, 2, 3]") == []
    assert _driver().parse_stream_line("") == []


# ── is_transient_error ─────────────────────────────────────────────────


def test_transient_error_allowlist() -> None:
    d = _driver()
    assert d.is_transient_error("429 rate limit exceeded") is True
    assert d.is_transient_error("Service Unavailable") is True
    assert d.is_transient_error("") is False
    assert d.is_transient_error("invalid API key") is False
