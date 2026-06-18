"""coordwake.py — extracted from coordination.py (daemon-architecture-v2 Phase 3d).

WakeMixin: methods moved VERBATIM out of CoordinationMixin; Daemon inherits both so
every self.* resolves on the combined instance -> byte-identical."""

from __future__ import annotations

from pathlib import Path
from typing import Dict, List, Optional

from prompts import _agent_type_from_conv_slug, _agent_type_normalised
from utils import _debug_emit, _log, parse_frontmatter


class WakeMixin:
    def _unfinished_dependencies(
        self,
        task_id: Optional[str],
        initiative_id: Optional[str],
    ) -> List[str]:
        """Read the target task's frontmatter and return the subset of
        `depends_on:` references whose current status is NOT `done`.
        Empty list = green light. Returns [] silently on any IO error
        (we don't want a missing file or a bad parse to deadlock the
        architect — the dispatch proceeds and the subagent will hit
        the real problem with a clearer error)."""
        if not task_id or not initiative_id:
            return []
        try:
            # Locate the task .md file under .meshkore/roadmap/initiatives/<init>/<task>.md
            # OR the legacy flat layout. Honour either.
            candidates = [
                self.paths.roadmap_dir
                / "initiatives"
                / initiative_id
                / f"{task_id}.md",
                self.paths.roadmap_dir / "tasks" / f"{task_id}.md",
            ]
            task_path: Optional[Path] = None
            for c in candidates:
                if c.exists():
                    task_path = c
                    break
            if task_path is None:
                return []
            raw = task_path.read_text(errors="replace")
            front = parse_frontmatter(raw)
            deps_raw = front.get("depends_on") if isinstance(front, dict) else None
            if not deps_raw:
                return []
            # Accept either a YAML list or a comma-separated string.
            if isinstance(deps_raw, str):
                deps = [s.strip() for s in deps_raw.split(",") if s.strip()]
            elif isinstance(deps_raw, list):
                deps = [str(s).strip() for s in deps_raw if str(s).strip()]
            else:
                return []
            if not deps:
                return []
            # Read current statuses from the state cache. Build a quick
            # task_id → status map; default to "unknown" (treated as
            # not-done, conservative).
            state = self.state_manager.state()
            status_by_id: Dict[str, str] = {}
            for t in (state.get("roadmap") or {}).get("tasks") or []:
                tid = t.get("id")
                if tid:
                    status_by_id[str(tid)] = str(t.get("status") or "unknown")
            missing: List[str] = []
            for dep in deps:
                if status_by_id.get(dep, "unknown") != "done":
                    missing.append(dep)
            return missing
        except Exception as e:
            _log(f"_unfinished_dependencies({task_id}) raised: {e}")
            return []

    def _maybe_wake_parent_architect(
        self,
        *,
        child_conv: str,
        child_agent_id: Optional[str],
        child_final_text: str,
        child_exit: Optional[int],
    ) -> None:
        """Architect-wake hook (initiative `architect-wake-on-subagent`).

        When a child conv emits `chat.assistant.final`, look up its
        recorded `parent_conv`. If the parent is a roadmap-architect
        conv, post a `[architect-wake]` user turn back to it so the
        pass resumes automatically. If the architect is mid-turn the
        wake is merged into its pending queue (chat_sessions.queue);
        if it has already exited the wake spawns a fresh turn. Both
        paths are correct and converge on the same outcome.

        py-1.10.24 — Wake message now annotates the outcome explicitly
        ('success' / 'no-commit' / 'error') + the cumulative unproductive
        count per task_id so the architect cannot ignore the
        DECISION MATRIX rule "Sub-agent failed twice → mark blocked".

        No-op when: no parent recorded; parent isn't a roadmap-architect
        conv; parent conv has been archived/cancelled. Quiet failures —
        a missing wake never blocks the child's final from being
        broadcast.
        """
        parent_conv = self._conv_meta_parent(child_conv)
        if not parent_conv:
            _debug_emit(
                "architect-wake.skipped",
                msg=f"no parent_conv recorded for {child_conv}",
                lvl="debug",
                conv=child_conv,
                agent_id=child_agent_id,
            )
            return
        parent_type = _agent_type_from_conv_slug(parent_conv)
        if parent_type != "roadmap-architect":
            # Wake hook is roadmap-architect-only for now. Generalising
            # to any parent type is on roadmap (would let custom agents
            # spawn worker children with auto-resume too) but needs
            # cycle-protection design first.
            _debug_emit(
                "architect-wake.skipped",
                msg=f"parent {parent_conv} is not a roadmap-architect conv",
                lvl="debug",
                conv=child_conv,
                data={"parent_conv": parent_conv, "parent_type": parent_type},
            )
            return
        # Build a compact wake message. Architect needs the child id +
        # a preview of the answer to know whether the task succeeded.
        preview = (child_final_text or "").strip()
        if len(preview) > 800:
            preview = preview[:800].rstrip() + " …(truncated)"
        agent_tag = f" ({child_agent_id})" if child_agent_id else ""
        exit_tag = f" exit={child_exit}" if child_exit not in (None, 0) else ""
        # py-1.10.24 — Classify the outcome + count failures per task.
        outcome = self._classify_subagent_final(preview, child_exit)
        # Pull the task_id the child was working on so we can name it
        # in the wake AND bump the counter.
        child_meta = self._conv_meta_load().get(child_conv) or {}
        task_id = child_meta.get("task_id") or None
        initiative_id = child_meta.get("initiative_id") or None
        child_agent_type = _agent_type_normalised(child_meta.get("agent_type"))
        fail_count = 0
        verdict_line = ""
        if outcome == "success":
            verdict_line = "VERDICT: ✓ success (commit detected in preview)"
        elif outcome == "rate-limited":
            # py-1.10.26 — Quota exhausted on the upstream CLI. Pause
            # the whole agent_type so the architect doesn't keep
            # throwing dispatches at a wall. Verdict tells it WHY
            # AND how long the cooldown lasts; matrix rule forces
            # mark-blocked-and-move-on (different from a normal fail
            # because no retry helps here).
            pause = self._pause_agent_type(
                child_agent_type,
                reason=f"rate-limited final from {child_conv}",
            )
            verdict_line = (
                f"VERDICT: ⏸ rate-limited — task `{task_id or '?'}` hit the "
                f"`{child_agent_type}` CLI quota. Agent type **paused until "
                f"{pause.get('expires_at')}**; further dispatches of this "
                f"type will return 503. **MATRIX RULE: mark this task "
                f"`blocked: rate-limited` and DO NOT retry — retrying does "
                f"not help until the quota window resets. You CAN dispatch "
                f"a DIFFERENT agent_type (deploy / db / testing / docs / "
                f"review) on other tasks while we wait.**"
            )
        else:
            fail_count = self._bump_task_failure(task_id)
            kind = (
                "no-commit (subagent didn't ship)"
                if outcome == "no-commit"
                else f"error (exit={child_exit})"
            )
            if fail_count >= 2:
                verdict_line = (
                    f"VERDICT: ✗ {kind} — task `{task_id or '?'}` has now "
                    f"failed {fail_count}× this session. **MATRIX RULE: "
                    f"sub-agent failed twice → mark this task `blocked` "
                    f"with the reason and MOVE ON. Do NOT retry a third "
                    f"time.**"
                )
            else:
                verdict_line = (
                    f"VERDICT: ✗ {kind} — task `{task_id or '?'}` fail #{fail_count}. "
                    f"One retry allowed by matrix; after that mark blocked."
                )
        task_tag = f" (init={initiative_id}, task={task_id})" if task_id else ""
        # py-1.10.25 — Pass-complete detection. When no active/next
        # initiative has any task left in {next, active, in_progress},
        # the architect is done and the wake forces the 4-bucket
        # end-of-pass summary instead of allowing more dispatches.
        pass_complete = self._roadmap_pass_complete()
        if pass_complete:
            continuation = (
                "**END-OF-PASS DETECTED.** The roadmap has NO remaining "
                "actionable tasks (every active/next initiative is either "
                "fully shipped or fully blocked). DO NOT dispatch more "
                "subagents. Emit the 4-bucket summary NOW (shipped / "
                "stubs-in-place / deferred-ops / decisions, + "
                "spec-needs-clarification if any), then end your turn. "
                "The pass is closed."
            )
        else:
            continuation = (
                "Continue the roadmap pass: apply the verdict, mark "
                "the originating task done/blocked accordingly, then dispatch "
                "the next wave (or emit the end-of-pass summary if everything "
                "actionable is shipped or blocked)."
            )
        wake_text = (
            f"[architect-wake] Subagent `{child_conv}`{agent_tag}{task_tag} finished{exit_tag}.\n\n"
            f"{verdict_line}\n\n"
            f"Result preview:\n{preview}\n\n"
            f"{continuation}"
        )
        _debug_emit(
            "architect-wake",
            msg=f"waking {parent_conv} on {outcome} of {child_conv}"
            + (f" (task {task_id} fail#{fail_count})" if fail_count else ""),
            conv=parent_conv,
            agent_id=child_agent_id,
            lvl=("warn" if outcome != "success" and fail_count >= 2 else "info"),
            data={
                "child_conv": child_conv,
                "child_exit": child_exit,
                "outcome": outcome,
                "task_id": task_id,
                "initiative_id": initiative_id,
                "task_fail_count": fail_count,
                "preview_len": len(preview),
                "preview_head": preview[:200],
            },
        )
        try:
            code, resp = self.chat_dispatch(
                {
                    "conv": parent_conv,
                    "text": wake_text,
                    "author": "architect-wake",
                    "agent_type": "roadmap-architect",
                }
            )
            if code >= 400:
                _log(
                    f"architect-wake dispatch to {parent_conv} returned {code}: {resp}"
                )
                _debug_emit(
                    "architect-wake.failed",
                    msg=f"chat_dispatch returned {code}",
                    lvl="warn",
                    conv=parent_conv,
                    data={"code": code, "resp": resp},
                )
        except Exception as e:
            _log(f"architect-wake dispatch raised for {parent_conv}: {e}")
            _debug_emit(
                "architect-wake.failed",
                msg=f"chat_dispatch raised: {e}",
                lvl="error",
                conv=parent_conv,
            )
        # py-1.11.0 — Re-broadcast the PARENT's activity. The wake just
        # re-dispatched the architect (live=true again) or queued it
        # (still live with pending merged). Child broadcast + child
        # auto-archive happen directly from the runner's emit-final
        # path so they fire even when there's no parent to wake.
        self._broadcast_conv_activity(parent_conv)
