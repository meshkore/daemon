"""runnerspawn.py — extracted from runner.py (daemon-architecture-v2 Phase 3d).

RunnerSpawnMixin: methods moved VERBATIM out of ChatRunner; Daemon inherits both so
every self.* resolves on the combined instance -> byte-identical.

DM-CLI-01 (multi-cli-clients) — spawn() no longer hardcodes the `claude`
binary/argv. It resolves a ClientDriver (`self.client`, default
claude-code) and delegates binary discovery + argv construction to it,
so a future member on `client: gemini`/`codex` spawns through the same
path with zero changes here."""

from __future__ import annotations

import os
import threading
import time

from clidrivers import driver_for
from runnerutil import _session_id_for_conv
from utils import _append_timeline, _iso_now, _log


class RunnerSpawnMixin:
    def spawn(self) -> None:
        import subprocess

        driver = driver_for(getattr(self, "client", None))
        self._driver_id = driver.id
        binary = driver.find_binary()
        if not binary:
            err = f"{driver.label} not found — {driver.install_hint()}"
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
        session_id = _session_id_for_conv(self.conv)
        # py-1.6.1 HOTFIX (claude-code specific, see ClaudeCodeDriver) —
        # --session-id caused empty assistant responses in production;
        # default off until re-tested. Computed here (driver-agnostic
        # conv→uuid hash) and passed through; only the claude-code
        # driver currently ever puts it in argv.
        use_session = os.environ.get("MESHKORE_CLAUDE_SESSION_ID", "").strip() in (
            "1",
            "true",
            "yes",
            "on",
        )
        briefing = self._briefing()
        args = driver.build_args(
            binary,
            prompt=briefing,
            model=self.model,
            effort=self.effort,
            session_id=session_id,
            use_session=use_session,
        )
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
        # QX5 — record HEAD at turn start so the resolution can diff
        # exactly what this turn changed (files + commits). Quiet on any
        # failure; resolution just records nothing then.
        try:
            _head = subprocess.run(
                ["git", "rev-parse", "HEAD"],
                cwd=str(self.paths.root),
                capture_output=True,
                text=True,
                timeout=5,
            )
            self._turn_start_sha = (
                _head.stdout.strip() if _head.returncode == 0 else None
            )
        except Exception:  # best-effort telemetry — never break the spawn
            self._turn_start_sha = None
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
        # Deliver the briefing (stdin-pipe by default; a driver may
        # override for a client that wants it delivered differently).
        driver.write_prompt(self.proc, briefing)
        _log(
            f"{driver.id}({self.conv}) spawned pid={self.pid} agent_type={self.agent_type} "
            f"stream={self.stream_id} briefing_len={len(briefing)}"
        )
        self.hub.broadcast(
            {
                "type": "task.started",
                "id": f"chat:{self.conv}",
                "agent": self.identity,
                "ts": _iso_now(),
                "runner": driver.id,
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
                _log(
                    f"{getattr(self, '_driver_id', 'claude-code')}({self.conv}) stderr: {line}"
                )
