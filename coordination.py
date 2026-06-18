"""coordination.py — extracted from daemon.py (daemon-architecture-v2 Phase 2).

CoordinationMixin: methods moved VERBATIM; Daemon inherits it so every self.*
still resolves on the combined instance -> byte-identical."""

from __future__ import annotations

from typing import Any, Dict, List, Optional, Tuple

from prompts import _agent_type_normalised


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
