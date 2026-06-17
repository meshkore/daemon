"""coordination.py — extracted from daemon.py (daemon-architecture-v2 Phase 2).

CoordinationMixin: methods moved VERBATIM; Daemon inherits it so every self.*
still resolves on the combined instance -> byte-identical."""

from __future__ import annotations

import re
import time
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

from cluster import normalize_status
from prompts import (
    AGENT_PROMPTS,
    _agent_manifest,
    _agent_type_from_conv_slug,
    _agent_type_normalised,
)
from utils import _debug_emit, _log, parse_frontmatter


class CoordinationMixin:
    def _dispatch_mutex_check(
        self,
        *,
        conv: str,
        agent_type: Optional[str],
        parent_conv: Optional[str],
        task_id: Optional[str],
        initiative_id: Optional[str] = None,
    ) -> Optional[Tuple[int, Dict[str, Any]]]:
        """py-1.10.25 — server-side enforcement of dispatch invariants
        the architect prompt claims but the LLM sometimes ignores.
        Returns `None` to allow the dispatch, or `(409, body)` to reject.

        Invariants enforced (both observed broken in cavioca 2026-05-30):

        1. **Single live roadmap-architect.** At most one
           `roadmap-architect-*` conv may have a live ChatRunner. A
           wake to the SAME conv is allowed (it's just the next turn
           on the existing architect); a dispatch to a DIFFERENT
           `roadmap-architect-*` while one is alive is refused.

        2. **No parallel dispatch on the same (parent_conv, task_id).**
           If the architect already dispatched task `T` and that conv
           is still streaming, a second dispatch on the same (parent,
           task) pair is refused. Prevents two subagents racing on
           the same file commits.

        The architect catches 409s on its bash tool and (per the
        prompt addendum below) should treat them as "wait for the
        wake, don't retry". Cockpit reads the `hint` and surfaces
        a soft notice.
        """
        # py-1.10.26 — Pause check FIRST. If the agent_type is in
        # cool-down because of a recent rate-limit hit, refuse 503
        # with a hint that names the ETA. Architect prompt update
        # below tells it to NOT retry — wait, or switch type.
        # `roadmap-architect` itself is exempted (we don't want to
        # lock the coordinator out of its own conv just because a
        # subagent hit a wall). The architect can still narrate +
        # dispatch other types or different convs.
        norm_target = _agent_type_normalised(agent_type)
        if norm_target != "roadmap-architect":
            pause = self._agent_type_is_paused(norm_target)
            if pause is not None:
                return 503, {
                    "error": "agent-type-paused",
                    "agent_type": norm_target,
                    "reason": pause.get("reason"),
                    "expires_at": pause.get("expires_at"),
                    "expires_epoch": pause.get("expires_epoch"),
                    "hint": (
                        f"Agent type `{norm_target}` is paused until "
                        f"{pause.get('expires_at')} (rate-limit cooldown). "
                        "Wait for the window to reset, switch to a "
                        "different agent_type, or `POST /agent-types/"
                        f"{norm_target}/unpause` to override."
                    ),
                }

        is_architect_target = (agent_type == "roadmap-architect") or conv.startswith(
            "roadmap-architect-"
        )
        live = self.chat_sessions.list_active()

        # Invariant 1: single live roadmap-architect.
        # Match BOTH by slug AND by stored agent_type in conv_meta —
        # the slug is the canonical signal for cockpit-spawned convs
        # but custom-named convs can also carry the agent_type via
        # /chat/dispatch body, and we must catch both.
        if is_architect_target:
            all_meta = self._conv_meta_load()
            others: List[str] = []
            for c in live:
                if c == conv:
                    continue
                slug_arch = c.startswith("roadmap-architect-")
                meta_arch = (
                    _agent_type_normalised((all_meta.get(c) or {}).get("agent_type"))
                    == "roadmap-architect"
                )
                if slug_arch or meta_arch:
                    others.append(c)
            if others:
                return 409, {
                    "error": "roadmap-architect-already-live",
                    "hint": (
                        "Another roadmap-architect conv is already running. "
                        "Stop it first (POST /chat/cancel) before spawning a new one."
                    ),
                    "existing_convs": others,
                    "requested_conv": conv,
                }

        # Invariants 2 + 3 both need the conv_meta sidecar.
        if parent_conv:
            all_meta = self._conv_meta_load()

            # Invariant 2: no parallel dispatch on same (parent_conv, task_id).
            # Only meaningful when both parent_conv and task_id are set —
            # i.e., the architect dispatching a subagent.
            if task_id:
                for live_conv in live:
                    if live_conv == conv:
                        continue
                    m = all_meta.get(live_conv) or {}
                    if (
                        m.get("parent_conv") == parent_conv
                        and m.get("task_id") == task_id
                    ):
                        return 409, {
                            "error": "task-already-dispatched",
                            "hint": (
                                f"Task `{task_id}` (parent `{parent_conv}`) "
                                f"already has a live dispatch: `{live_conv}`. "
                                "Wait for the [architect-wake] on its final; "
                                "do not retry while it's still running."
                            ),
                            "existing_conv": live_conv,
                            "parent_conv": parent_conv,
                            "task_id": task_id,
                        }

            # Invariant 3 (py-1.10.28): single initiative in-flight per
            # architect. Operator's product decision (2026-05-31): "una
            # iniciativa a la vez, tareas en paralelo DENTRO pero no
            # mezclando entre iniciativas". The architect is allowed
            # to dispatch parallel tasks within initiative I, but
            # cannot start I+1 while ANY task on I still has a live
            # subagent. The 409 hint names the live initiative(s) so
            # the architect knows what it's waiting on. Linear-roadmap
            # mode prevents half-finished initiatives + reduces quota
            # burn on speculative parallel work.
            if initiative_id:
                live_initiatives: set = set()
                for live_conv in live:
                    if live_conv == conv:
                        continue
                    m = all_meta.get(live_conv) or {}
                    if m.get("parent_conv") != parent_conv:
                        continue
                    other = m.get("initiative_id")
                    if other:
                        live_initiatives.add(other)
                if live_initiatives and initiative_id not in live_initiatives:
                    return 409, {
                        "error": "initiative-already-in-flight",
                        "hint": (
                            "Linear-roadmap mode: another initiative still "
                            f"has live subagents (`{', '.join(sorted(live_initiatives))}`). "
                            f"Wait for ALL its tasks to finish (or mark them "
                            f"blocked) before dispatching into "
                            f"`{initiative_id}`. Parallel work is allowed "
                            "INSIDE a single initiative, never across."
                        ),
                        "live_initiatives": sorted(live_initiatives),
                        "requested_initiative": initiative_id,
                        "parent_conv": parent_conv,
                    }

        # py-1.12.0 — Worker-dispatch invariants. Only fire when this
        # dispatch is creating/touching a `work-*` subagent slot. The
        # architect's own dispatches (roadmap-architect-*) and the
        # operator's free-form custom convs sidestep these checks —
        # they're not "worker dispatches", they're conversation starts.
        is_worker_dispatch = conv.startswith("work-")
        if is_worker_dispatch:
            # Invariant 5: required join keys. work-* dispatches MUST
            # carry both `initiative_id` AND `task_id` so that
            # Invariants 2+3 actually fire. Pre-py-1.12.0 a dispatch
            # missing either field would silently slip past the
            # mutex (line 6325 was guarded by `if task_id:`, line 6354
            # by `if initiative_id:`). The architect prompt already
            # requires both fields; this turns "should send" into
            # "must send" with a clear 400 if it forgets.
            if not initiative_id or not task_id:
                missing = []
                if not initiative_id:
                    missing.append("initiative_id")
                if not task_id:
                    missing.append("task_id")
                return 400, {
                    "error": "worker-dispatch-missing-join-keys",
                    "missing": missing,
                    "hint": (
                        f"`{conv}` is a work-* subagent dispatch — it MUST "
                        f"include both `initiative_id` AND `task_id` in the "
                        f"POST body so the daemon can enforce linear-init + "
                        f"depends_on. Missing: {', '.join(missing)}. Re-read "
                        f"the SOP `EXECUTION LOOP — LINEAR INITIATIVES` block."
                    ),
                }

            # Invariant 4: wave cap. The architect prompt promises
            # "max 3 parallel"; enforce it here so a runaway loop or a
            # confused turn can't spawn 7 workers and 5x the quota burn.
            # Cap is configurable via cluster.yaml.architect.wave_cap;
            # default 3 (matches the prompt). Per-parent_conv so two
            # operators on the same cluster (different architect convs)
            # each get their own wave budget.
            cap = self._wave_cap()
            if parent_conv:
                same_wave = 0
                all_meta_w = self._conv_meta_load()
                for live_conv in live:
                    if live_conv == conv:
                        continue
                    if not live_conv.startswith("work-"):
                        continue
                    m = all_meta_w.get(live_conv) or {}
                    if m.get("parent_conv") == parent_conv:
                        same_wave += 1
                if same_wave >= cap:
                    return 429, {
                        "error": "wave-cap-reached",
                        "wave_cap": cap,
                        "current_wave_size": same_wave,
                        "parent_conv": parent_conv,
                        "hint": (
                            f"This architect already has {same_wave} work-* "
                            f"subagent(s) in flight (cap={cap}). Wait for a "
                            f"slot to free up via [architect-wake] before "
                            f"dispatching the next task. Operator can raise "
                            f"the cap via `cluster.yaml.architect.wave_cap` "
                            f"(higher = faster, more quota burn + more "
                            f"chance of git-race)."
                        ),
                    }

            # Invariant 6: depends-on gate. Refuse the dispatch if the
            # target task's `depends_on:` frontmatter lists upstream
            # tasks that are NOT marked `done`. The architect should
            # already serialise via depends_on at the prompt level —
            # this is the server-side belt to the prompt's braces.
            # Cheap: reads one task .md file + checks the upstream
            # statuses we already cache in `_state['roadmap']['tasks']`.
            missing_deps = self._unfinished_dependencies(task_id, initiative_id)
            if missing_deps:
                return 409, {
                    "error": "task-dependencies-not-done",
                    "task_id": task_id,
                    "initiative_id": initiative_id,
                    "missing": missing_deps,
                    "hint": (
                        f"Task `{task_id}` declares `depends_on: "
                        f"{missing_deps}` in its frontmatter but those "
                        f"upstream task(s) are not `done` yet. Finish "
                        f"them first (or remove the dependency if it's "
                        f"stale). Do NOT retry this dispatch until then."
                    ),
                }

        return None

    def _wave_cap(self) -> int:
        """Return the per-architect parallel-worker cap. Read from
        cluster.yaml.architect.wave_cap; default 3 (matches the
        roadmap-architect prompt's stated bound). Operator can widen
        for throughput or narrow for cost."""
        try:
            raw = (self.cluster.data.get("architect") or {}).get("wave_cap")
            if raw is None:
                return 3
            n = int(raw)
            return max(1, min(10, n))  # clamp to a sane range
        except Exception:
            return 3

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
