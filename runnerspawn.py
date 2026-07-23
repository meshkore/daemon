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
from providers import CLIENT_KEY_SPECS, build_launch_env
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
        # multi-provider-agents (MPV1) — build the launch env PROVIDER-aware
        # for the claude-code client (the only client with a provider dial).
        # For every other client, env stays today's behavior. The provider
        # env is built PER INSTANCE from a scrubbed copy so an Anthropic turn
        # can never inherit a stray ZAI base-url/token from the daemon's own
        # shell (and vice versa) — see providers.build_launch_env.
        base_env = dict(os.environ)
        if driver.id == "claude-code":
            provider_id = getattr(self, "provider", None) or "anthropic"
            daemon = getattr(self, "daemon", None)
            resolved = None
            if daemon is not None and hasattr(daemon, "resolve_provider"):
                try:
                    resolved = daemon.resolve_provider(provider_id)
                except Exception as e:  # config trouble must never crash a spawn
                    _log(f"provider resolve failed for {provider_id!r}: {e}")
                    resolved = None
            # A non-default provider that needs a key but has none is a hard
            # config error — abort with a clear message rather than silently
            # falling back to Anthropic (which would spend the WRONG quota).
            if provider_id != "anthropic" and not (
                resolved and resolved.get("available")
            ):
                err = (
                    f"provider {provider_id!r} is not usable — set its API key in "
                    "the cockpit's General settings (⚙, top-right) or disable it"
                )
                _log(f"{driver.id}({self.conv}) {err}")
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
            # Thread the per-turn model in so build_launch_env can set
            # ANTHROPIC_MODEL alongside the --model argv (identical value).
            resolved = {**(resolved or {}), "model": self.model}
            env = build_launch_env(base_env, provider_id, resolved=resolved)
        else:
            env = base_env
            # multi-provider-agents follow-up — Codex/Gemini aren't claude-
            # code providers; they're CLIENTS that may have a daemon-managed
            # API key (Config → General settings) so headless agents don't
            # depend on an interactive `codex login` / `gcloud auth` having
            # already happened in this shell. No scrubbing needed here (no
            # cross-contamination risk like Anthropic/ZAI sharing one env-
            # var family) — only overlay when a key is actually stored;
            # absent one, the client's own native login/env is unchanged.
            if driver.id in CLIENT_KEY_SPECS:
                daemon = getattr(self, "daemon", None)
                if daemon is not None and hasattr(daemon, "resolve_client_key"):
                    try:
                        key = daemon.resolve_client_key(driver.id)
                    except Exception as e:  # config trouble must never crash a spawn
                        _log(f"client key resolve failed for {driver.id!r}: {e}")
                        key = None
                    if key:
                        env = dict(env)
                        env[CLIENT_KEY_SPECS[driver.id]["env_var"]] = key
        env["MESHKORE_IDENTITY"] = self.identity
        env["MESHKORE_CONV"] = self.conv
        env["MESHKORE_SESSION_ID"] = session_id
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
