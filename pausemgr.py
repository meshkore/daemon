"""pausemgr.py — extracted from coordination.py (daemon-architecture-v2 Phase 3d).

PauseMixin: methods moved VERBATIM out of CoordinationMixin; Daemon inherits both so
every self.* resolves on the combined instance -> byte-identical."""

from __future__ import annotations

import re
import time
from typing import Any, Dict, List, Optional

from cluster import normalize_status
from prompts import AGENT_PROMPTS, _agent_manifest
from utils import _debug_emit, _log


class PauseMixin:
    def _classify_subagent_final(self, preview: str, exit_code: Optional[int]) -> str:
        """Return one of:
            'success'     — committed work landed AND `git cat-file -e` confirms the sha
            'no-commit'   — turn ended with no commit hash in preview, OR the
                             claimed hash doesn't exist in the repo (py-1.12.0
                             Invariant 7 — ghost commit detection)
            'error'       — non-zero exit (CLI crashed or was killed)
            'rate-limited' — upstream CLI told us the quota is out
        Rate-limit detection runs first because some CLIs report the
        condition with exit=0 + a polite "try again later" message,
        which would otherwise look like a normal `no-commit`."""
        text = preview or ""
        if any(p.search(text) for p in self._RATE_LIMIT_PATTERNS):
            return "rate-limited"
        if exit_code not in (None, 0):
            return "error"
        commit_match = False
        for pat in self._COMMIT_PATTERNS:
            m = pat.search(text)
            if not m:
                continue
            # py-1.12.0 Invariant 7 — verify the claimed sha exists in
            # the repo. Subagents occasionally hallucinate commit
            # hashes; without this check the architect would mark the
            # task `done` and move on, leaving the work undone forever.
            # If the pattern doesn't capture a sha (e.g. the ✓-line
            # pattern) we still trust it — the prompt mandates the
            # commit line too, and we don't want false negatives from
            # a pattern that's intentionally permissive.
            sha = m.group(1) if m.lastindex and m.lastindex >= 1 else None
            if sha and not self._git_commit_exists(sha):
                _log(
                    f"classify: subagent claimed commit {sha} but it doesn't exist in repo — demoting to no-commit"
                )
                _debug_emit(
                    "subagent-final.ghost-commit",
                    msg=f"claimed commit {sha} does not exist",
                    lvl="warn",
                    data={"claimed_sha": sha, "preview_head": text[:200]},
                )
                continue
            commit_match = True
            break
        return "success" if commit_match else "no-commit"

    def _git_commit_exists(self, sha: str) -> bool:
        """Run `git cat-file -e <sha>` from the project root. Returns
        True iff the sha is a valid object in the repo. Silently False
        on any error (no git binary, not a repo, etc.) — the architect
        will get the 'no-commit' verdict and the task fail-counter will
        bump, which is the correct safe default."""
        if not re.match(r"^[0-9a-f]{6,40}$", sha):
            return False
        try:
            import subprocess

            r = subprocess.run(
                ["git", "cat-file", "-e", sha],
                cwd=str(self.paths.root),
                stdout=subprocess.DEVNULL,
                stderr=subprocess.DEVNULL,
                timeout=5,
            )
            return r.returncode == 0
        except Exception:
            return False

    def _roadmap_pass_complete(self) -> bool:
        """py-1.10.25 — True iff no active/next initiative has any
        non-terminal task left. Used by the architect-wake hook to
        flip the message into "emit the summary and stop" mode when
        the pass has run out of actionable work.

        Terminal task statuses (count as 'done for the pass'):
        `done`, `blocked`, `cancelled`. Anything else (`next`,
        `active`, `in_progress`, `pending-operator`, …) still needs
        an agent.

        Falls back to False on any error — better to keep retrying
        than to falsely terminate a live pass."""
        try:
            snap = self.state_manager.state()
            inits = snap.get("initiatives") or []
            tasks_by_init: Dict[str, List[Dict[str, Any]]] = {}
            for t in (snap.get("roadmap") or {}).get("tasks") or []:
                iid = t.get("initiative")
                if iid:
                    tasks_by_init.setdefault(iid, []).append(t)
            terminal = {"done", "blocked", "cancelled"}
            for it in inits:
                status = normalize_status(it.get("status"))
                if status in ("done", "backlog"):
                    continue
                # active/next/in_progress — does it have actionable tasks?
                kids = tasks_by_init.get(it.get("id"), [])
                if any(normalize_status(k.get("status")) not in terminal for k in kids):
                    return False
            return True
        except Exception:
            return False

    def _pause_agent_type(
        self,
        agent_type: str,
        *,
        reason: str,
        duration_secs: Optional[int] = None,
    ) -> Dict[str, Any]:
        """Pause the quota pool that `agent_type` belongs to. Multiple
        types sharing a platform+model pause together (that's the
        point — they share the same upstream account)."""
        if not agent_type:
            return {}
        m = _agent_manifest(agent_type)
        entry = self.quota.pause(
            m["quota_key"],
            reason=reason,
            duration_secs=duration_secs,
            platform=m["platform"],
            model=m["model"],
        )
        # Back-compat shape for existing cockpit reader (until V108 lands).
        return {
            "since": entry.get("paused_at"),
            "epoch": entry.get("paused_until_epoch", 0)
            - (entry.get("paused_until_epoch", 0) - int(time.time())),
            "expires_at": entry.get("paused_until"),
            "expires_epoch": entry.get("paused_until_epoch"),
            "reason": entry.get("reason"),
            "duration_secs": duration_secs,
            "quota_key": m["quota_key"],
            "platform": m["platform"],
            "model": m["model"],
        }

    def _unpause_agent_type(self, agent_type: str) -> bool:
        if not agent_type:
            return False
        m = _agent_manifest(agent_type)
        return self.quota.unpause(m["quota_key"])

    def _agent_type_is_paused(
        self, agent_type: Optional[str]
    ) -> Optional[Dict[str, Any]]:
        if not agent_type:
            return None
        m = _agent_manifest(agent_type)
        if not self.quota.is_paused(m["quota_key"]):
            return None
        entry = self.quota.get(m["quota_key"]) or {}
        return {
            "expires_at": entry.get("paused_until"),
            "expires_epoch": entry.get("paused_until_epoch"),
            "reason": entry.get("reason"),
            "quota_key": m["quota_key"],
            "platform": m["platform"],
            "model": m["model"],
        }

    def _paused_agent_types_view(self) -> Dict[str, Dict[str, Any]]:
        """Back-compat projection: map every agent_type whose
        quota_key is paused onto the legacy /health field shape.
        Built by walking AGENT_PROMPTS so multiple types sharing a
        pool all appear paused together (correct — they actually are)."""
        paused = self.quota.paused_view()
        out: Dict[str, Dict[str, Any]] = {}
        for t in AGENT_PROMPTS.keys():
            m = _agent_manifest(t)
            entry = paused.get(m["quota_key"])
            if entry:
                out[t] = {
                    "since": entry.get("paused_at"),
                    "expires_at": entry.get("paused_until"),
                    "expires_epoch": entry.get("paused_until_epoch"),
                    "reason": entry.get("reason"),
                    "quota_key": m["quota_key"],
                    "platform": entry.get("platform"),
                    "model": entry.get("model"),
                    "consecutive_rate_limits": entry.get("consecutive_rate_limits", 0),
                }
        return out

    def _bump_task_failure(self, task_id: Optional[str]) -> int:
        """Increment + return the cumulative unproductive-final count
        for `task_id` since daemon boot. Returns 0 when task_id is
        missing (untrackable)."""
        if not task_id:
            return 0
        if not hasattr(self, "_task_failures"):
            self._task_failures: Dict[str, int] = {}
        self._task_failures[task_id] = self._task_failures.get(task_id, 0) + 1
        return self._task_failures[task_id]
