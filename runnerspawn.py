"""runnerspawn.py — extracted from runner.py (daemon-architecture-v2 Phase 3d).

RunnerSpawnMixin: methods moved VERBATIM out of ChatRunner; Daemon inherits both so
every self.* resolves on the combined instance -> byte-identical."""

from __future__ import annotations

import os
import threading
import time

from runnerutil import _find_claude, _session_id_for_conv
from utils import _append_timeline, _iso_now, _log


class RunnerSpawnMixin:
    def spawn(self) -> None:
        import subprocess

        claude_bin = _find_claude()
        if not claude_bin:
            err = "claude CLI not found — install via `npm i -g @anthropic-ai/claude-code`"
            _log(err)
            self.hub.broadcast(
                _append_timeline(
                    self.paths,
                    {
                        "type": "chat.assistant.final",
                        "author": self.identity,
                        "conv": self.conv,
                        "stream_id": self.stream_id,
                        "text": f"[runner error] {err}",
                    },
                )
            )
            self.done.set()
            return
        # py-1.6.1 HOTFIX — --session-id from py-1.6.0 caused empty
        # assistant responses in production (claude-code exited
        # silently on subsequent turns of the same conv). Reverted to
        # opt-in via env var MESHKORE_CLAUDE_SESSION_ID=1. Default off
        # until the failure mode is understood and re-tested.
        # The uuid5 helper is preserved so reintroduction is a one-line
        # flip once safe.
        session_id = _session_id_for_conv(self.conv)
        use_session = os.environ.get("MESHKORE_CLAUDE_SESSION_ID", "").strip() in (
            "1",
            "true",
            "yes",
            "on",
        )
        args = [
            claude_bin,
            "-p",
            "--output-format",
            "stream-json",
            "--verbose",
            "--include-partial-messages",
            "--permission-mode",
            "bypassPermissions",
            # Headless: cockpit has no UI to surface interactive question
            # tools. Disallow them so the model defaults to plain-text
            # asks in the chat bubble instead of stalling on a hanging
            # AskUserQuestion / ExitPlanMode call.
            "--disallowed-tools",
            "AskUserQuestion,ExitPlanMode",
        ]
        # MP1 (py-1.13.3) — Per-conv model override. `--model` accepts
        # one of `opus` / `sonnet` / `haiku` or an explicit model id
        # (claude-opus-4-7, etc.). When unset (`auto` / None), we omit
        # the flag entirely and let claude-code pick its default.
        if self.model:
            args.extend(["--model", self.model])
        # MP3 (py-1.13.4) — reasoning-depth dial. Omitted when None
        # ('default' sentinel) so claude-code uses its own default.
        if self.effort:
            args.extend(["--effort", self.effort])
        if use_session:
            args[2:2] = ["--session-id", session_id]
        # py-1.10.5 — Pipe the briefing through stdin instead of
        # appending it as a positional argument. claude 2.1.145
        # rejects a trailing positional that arrives AFTER a
        # multi-value flag (`--disallowed-tools <comma,list>`) — the
        # parser consumes our prompt as another disallowed-tool name
        # or just drops it, and claude exits 1 with stderr:
        #   "Error: Input must be provided either through stdin or
        #    as a prompt argument when using --print"
        # Captured 2026-05-29 by py-1.10.4's stderr drainer (which
        # had been silently dropping this error for every spawn
        # since the cockpit's roadmap-architect feature shipped).
        # Stdin works regardless of argv order, so it's the
        # forward-compatible answer.
        briefing = self._briefing()
        env = {
            **os.environ,
            "MESHKORE_IDENTITY": self.identity,
            "MESHKORE_CONV": self.conv,
            "MESHKORE_SESSION_ID": session_id,
        }
        # Stamped so ChatSessionReaper can apply the hard-timeout check
        # (any runner whose runtime exceeds the reaper's threshold gets
        # force-cancelled). Set BEFORE Popen so even a subprocess that
        # hangs in the OS spawn path gets the timestamp.
        self._started_at = time.time()
        self.proc = subprocess.Popen(
            args,
            cwd=str(self.paths.root),
            env=env,
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            start_new_session=True,
        )
        self.pid = self.proc.pid
        # Write the briefing to stdin and close. claude reads it
        # all (EOF on close) then begins streaming results to stdout.
        try:
            if self.proc.stdin is not None:
                self.proc.stdin.write(briefing.encode("utf-8"))
                self.proc.stdin.close()
        except (BrokenPipeError, OSError) as e:
            _log(f"claude({self.conv}) stdin write failed: {e}")
        _log(
            f"claude({self.conv}) spawned pid={self.pid} agent_type={self.agent_type} "
            f"stream={self.stream_id} briefing_len={len(briefing)}"
        )
        self.hub.broadcast(
            {
                "type": "task.started",
                "id": f"chat:{self.conv}",
                "agent": self.identity,
                "ts": _iso_now(),
                "runner": "claude-code",
                "conv": self.conv,
                "stream_id": self.stream_id,
            }
        )
        # Empty assistant bubble so the cockpit shows progress immediately.
        self.hub.broadcast(
            {
                "type": "chat.assistant.delta",
                "author": self.identity,
                "conv": self.conv,
                "stream_id": self.stream_id,
                "text": "",
                "ts": _iso_now(),
            }
        )
        threading.Thread(target=self._reader_loop, daemon=True).start()
        # py-1.10.4 — stderr drainer. Until this lands, stderr=PIPE
        # was capturing claude's error output but NOBODY READ IT, so
        # every subprocess crash (prompt too long, blocked tool, env
        # issue, segfault) surfaced as "empty chat.assistant.final"
        # with no diagnostic anywhere in the daemon log. The reader
        # loop above only iterates stdout; PIPE'd stderr fills its
        # OS buffer (typically 64 KB) and on overflow Linux/Darwin
        # block claude on its next write — turning a soft failure
        # into an unkillable zombie. Drain it into the daemon log.
        threading.Thread(target=self._stderr_drain, daemon=True).start()

    def _stderr_drain(self) -> None:
        """Read self.proc.stderr line-by-line and forward to the
        daemon log. Tagged with conv so multiple in-flight runners
        don't blur together. Cheap — claude rarely emits much on
        stderr unless it's failing."""
        if not self.proc or not self.proc.stderr:
            return
        for raw in self.proc.stderr:
            try:
                line = raw.decode("utf-8", "replace").rstrip()
            except Exception:
                continue
            if line:
                _log(f"claude({self.conv}) stderr: {line}")
