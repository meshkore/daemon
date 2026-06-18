"""Chat-dispatch happy path — the core agent-turn flow, end to end, with a
FAKE `claude` CLI so no network / real model is needed.

The daemon spawns `claude -p --output-format stream-json …` (runnerspawn) and
parses its stdout stream (runnerloop). conftest puts `work/bin` first on the
daemon's PATH, so dropping an executable `claude` there makes
runnerutil._find_claude pick it up. The fake drains the briefing on stdin and
emits one terminal `result` event — exactly what _reader_loop finalises into a
`chat.assistant.final` timeline event.

This exercises the real dispatch → spawn → subprocess → stream-parse → finalise
chain that the route-warranty + characterization tests can't (they stop at
"route wired" / "argv shape").
"""

from __future__ import annotations

import time

import pytest

from conftest import Daemon

FAKE_REPLY = "FAKE_CLAUDE_REPLY_OK_42"

# A fake `claude`: drain stdin (the briefing — must read it or the daemon's
# stdin write could block), then emit the stream-json terminal `result` event
# the daemon's reader loop finalises on.
FAKE_CLAUDE = f"""#!/usr/bin/env python3
import sys, json
sys.stdin.read()
print(json.dumps({{
    "type": "result",
    "result": {FAKE_REPLY!r},
    "usage": {{"input_tokens": 5, "output_tokens": 7,
              "cache_read_input_tokens": 0, "cache_creation_input_tokens": 0}},
}}), flush=True)
"""


def _install_fake_claude(daemon: Daemon) -> None:
    binp = daemon.work / "bin" / "claude"
    binp.write_text(FAKE_CLAUDE)
    binp.chmod(0o755)


def _wait_final(daemon: Daemon, conv: str, timeout: float = 20.0) -> dict:
    """Poll the conv's messages until a chat.assistant.final lands."""
    deadline = time.time() + timeout
    last = None
    while time.time() < deadline:
        r = daemon.get(f"/chat/conv/{conv}/messages", headers=daemon.auth)
        if r.status_code == 200:
            last = r.json()
            for ev in last.get("messages", []):
                if ev.get("type") == "chat.assistant.final":
                    return ev
        time.sleep(0.25)
    raise AssertionError(
        f"no chat.assistant.final for {conv} within {timeout}s; last={last}"
    )


@pytest.mark.cluster("populated")
def test_chat_dispatch_happy_path(daemon: Daemon) -> None:
    _install_fake_claude(daemon)
    conv = "itest-happy"

    r = daemon.post(
        "/chat/dispatch",
        headers=daemon.auth,
        json={"conv": conv, "text": "ping"},
    )
    assert r.status_code in (200, 202), f"dispatch: {r.status_code} {r.text}"

    final = _wait_final(daemon, conv)
    assert final["text"].strip() == FAKE_REPLY, f"unexpected final: {final!r}"
    assert final.get("conv") == conv

    # The user turn was persisted too (chat.user before the final).
    msgs = daemon.get(f"/chat/conv/{conv}/messages", headers=daemon.auth).json()[
        "messages"
    ]
    kinds = [m.get("type") for m in msgs]
    assert "chat.user" in kinds, f"user turn missing: {kinds}"

    # After finalisation the conv is no longer live (the runner exited).
    deadline = time.time() + 10.0
    while time.time() < deadline:
        convs = daemon.get("/chat/convs", headers=daemon.auth).json()["convs"]
        me = next((c for c in convs if c["conv"] == conv), None)
        if me and not me["live"]:
            break
        time.sleep(0.25)
    else:
        pytest.fail(f"conv {conv} still live after the turn finished")


@pytest.mark.cluster("populated")
def test_chat_dispatch_then_cancel_is_clean(daemon: Daemon) -> None:
    """A dispatched conv can be cancelled; the daemon stays healthy."""
    _install_fake_claude(daemon)
    conv = "itest-cancel"
    daemon.post("/chat/dispatch", headers=daemon.auth, json={"conv": conv, "text": "x"})
    # cancel is idempotent + must not 500 even if the turn already finished
    r = daemon.post("/chat/cancel", headers=daemon.auth, json={"conv": conv})
    assert r.status_code in (200, 404), f"cancel: {r.status_code} {r.text}"
    assert daemon.get("/health").status_code == 200
