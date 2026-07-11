"""anchorprogress.py — extracted from anchor.py (daemon-architecture-v2 Phase 3d).

AnchorProgressMixin: methods moved VERBATIM out of AnchorMixin; Daemon inherits both so
every self.* resolves on the combined instance -> byte-identical."""

from __future__ import annotations

import re
import subprocess
from typing import Any, Dict, List, Optional, Tuple

from cluster import _patch_frontmatter
from prompts import _agent_type_from_conv_slug, _agent_type_normalised
from utils import _iso_now, _log, parse_frontmatter

# Standard v26 — cap the persisted resolution so a runaway final can't
# bloat the task file. The Output Contract keeps summaries small anyway.
_RESOLUTION_MAX_CHARS = 4000

# QX5 — bound the files/commits we record per task so an outsized turn
# (or a noisy parallel window) can't bloat the task file.
_MAX_FILES = 60
_MAX_COMMITS = 25


def _git_lines(root: Any, args: List[str]) -> List[str]:
    """Run a git command under `root`, return non-empty stdout lines.
    Quiet on any failure (no git, detached, bad range) — returns []."""
    try:
        proc = subprocess.run(
            ["git", *args],
            cwd=str(root),
            capture_output=True,
            text=True,
            timeout=8,
        )
        if proc.returncode != 0:
            return []
        return [ln.strip() for ln in proc.stdout.splitlines() if ln.strip()]
    except (OSError, subprocess.SubprocessError):
        return []


def _changes_since(root: Any, start_sha: Optional[str]) -> Tuple[List[str], List[str]]:
    """Files changed + commit SHAs between `start_sha` and HEAD. Returns
    (files, commit_shas), each capped. Empty when start_sha is missing or
    nothing changed (a turn that committed nothing → no registry noise)."""
    if not start_sha:
        return [], []
    rng = f"{start_sha}..HEAD"
    commits = _git_lines(root, ["log", rng, "--format=%H"])
    if not commits:
        return [], []  # nothing was committed this turn
    files = _git_lines(root, ["diff", "--name-only", rng])
    # Stable order, deduped (diff already unique, but be safe).
    seen: set = set()
    uniq_files = [f for f in files if not (f in seen or seen.add(f))]
    return uniq_files[:_MAX_FILES], commits[:_MAX_COMMITS]


def _fmt_tokens(n: int) -> str:
    """Compact token count: 128234 → '128k', 950 → '950'."""
    if n >= 1_000_000:
        return f"{n / 1_000_000:.1f}M".replace(".0M", "M")
    if n >= 1_000:
        return f"{round(n / 1000)}k"
    return str(n)


def _resolution_facts(files: List[str], commits: List[str], total_tokens: int) -> str:
    """The one-line facts strip + the files block appended below a
    resolution summary. English (Standard: product strings are English).

    Returns '' when there's nothing durable to record, so a turn that
    committed nothing and reported no tokens leaves a clean summary.

    Shape:
        **Commit** `a1b2c3d4e` (+2) · 12 files · 128k tokens

        **Files changed (12):**
        - `apps/web/x.tsx`
        - …
    """
    strip: List[str] = []
    if commits:
        more = f" (+{len(commits) - 1})" if len(commits) > 1 else ""
        strip.append(f"**Commit** `{commits[0][:9]}`{more}")
    if files:
        strip.append(f"{len(files)} file{'s' if len(files) != 1 else ''}")
    if total_tokens > 0:
        strip.append(f"{_fmt_tokens(total_tokens)} tokens")

    out = ""
    if strip:
        out += " · ".join(strip)
    if files:
        listed = "\n".join(f"- `{f}`" for f in files)
        out += f"\n\n**Files changed ({len(files)}):**\n{listed}"
    return out


def _upsert_body_section(text: str, heading: str, content: str) -> str:
    """Replace the `## <heading>` section (heading line through the line
    before the next `## ` or EOF) with `content`, or append it if absent.
    Everything else in the file — frontmatter, the original spec body —
    is preserved verbatim."""
    section = f"## {heading}\n\n{content.rstrip()}\n"
    pattern = re.compile(rf"^## {re.escape(heading)}[ \t]*$.*?(?=^## |\Z)", re.M | re.S)
    if pattern.search(text):
        return pattern.sub(section, text, count=1)
    sep = "" if text.endswith("\n") else "\n"
    return f"{text}{sep}\n{section}"


class AnchorProgressMixin:
    def _handle_anchor_progress(
        self, conv: str, payload: Dict[str, Any], *, raw: str
    ) -> None:
        """Mid-turn task transition. Writes `status: done` (or whatever
        the payload specifies) to the task .md and broadcasts."""
        tid = str(payload.get("t") or "").strip()
        new_status = str(payload.get("status") or "done").strip()
        if not self._TASK_ID_RE.match(tid):
            _log(f"anchor-progress: invalid task id {tid!r} from conv {conv}")
            return
        path = self._find_task(tid)
        if not path:
            _log(f"anchor-progress: task {tid} not found on disk (conv {conv})")
            return
        try:
            text = path.read_text()
            today = _iso_now()[:10]
            new = re.sub(
                r"^status:\s*\S+\s*$",
                f"status: {new_status}",
                text,
                count=1,
                flags=re.M,
            )
            if new == text:
                # No status line — insert after the opening ---
                new = re.sub(
                    r"^---\s*$\n",
                    f"---\nstatus: {new_status}\n",
                    text,
                    count=1,
                    flags=re.M,
                )
            new = re.sub(r"^updated:.*$", f"updated: {today}", new, count=1, flags=re.M)
            if new == text and "updated:" not in new:
                # No updated line — append within frontmatter
                new = re.sub(
                    r"(---\s*$)", f"updated: {today}\n\\1", new, count=1, flags=re.M
                )
            path.write_text(new)
        except Exception as e:
            _log(f"anchor-progress: write failed for task {tid}: {e}")
            return
        try:
            self.hub.broadcast(
                {
                    "type": "conv.task_completed",
                    "conv": conv,
                    "task_id": tid,
                    "new_status": new_status,
                    "ts": _iso_now(),
                }
            )
            self.state_manager.rebuild(broadcast=True)
        except Exception as e:
            _log(f"task_completed broadcast/rebuild failed: {e}")

    def _persist_task_resolution(
        self,
        *,
        conv: str,
        agent_id: Optional[str],
        final_text: str,
        exit_code: Optional[int],
        start_sha: Optional[str] = None,
    ) -> None:
        """Standard v26 — durable per-task resolution record.

        Called from the runner's emit-final path for EVERY conv. If the
        conv was anchored to a task and that task is now `done`, stamp the
        task .md with `completed_at` / `resolved_by` / `resolved_by_conv`
        and write the turn's Output-Contract summary into the
        `## Resolution` body section. Quiet on any failure — persistence
        must never block the final broadcast.

        CTX/QX — records a resolution for BOTH outcomes so the cockpit's RES
        line always has content + the right colour:
          • SUCCESS (exit 0 + status `done`) → `completed_at`/`resolved_by` +
            the Output-Contract summary (cockpit paints it BLUE).
          • FAILURE (non-zero exit) → mark the task `blocked` + `failed_at` and
            write the failure reason into `## Resolution` (cockpit paints it
            RED — "hay que atenderlo"), UNLESS the task is already `done` (a
            late non-zero on resolved work must not reopen it).
        Skips only when the conv has no anchored task, the task file is gone,
        or (success branch) the agent didn't mark it `done`. Quiet on any
        failure — persistence must never block the final broadcast.
        """
        try:
            meta = self._conv_meta_load().get(conv) or {}
            tid = (meta.get("task_id") or "").strip()
            if not tid or not self._TASK_ID_RE.match(tid):
                return
            path = self._find_task(tid)
            if not path:
                return
            text = path.read_text(errors="replace")
            fm = parse_frontmatter(text)
            status = str(fm.get("status") or "").strip().lower()
            now = _iso_now()
            summary = (final_text or "").strip()

            # Cumulative tokens spent on this task's conv (input + output +
            # both cache buckets). 0 when the conv has no recorded turns.
            total_tokens = 0
            try:
                usage = self.chat_sessions.usage_total(conv) or {}
                total_tokens = (
                    int(usage.get("input_tokens", 0) or 0)
                    + int(usage.get("output_tokens", 0) or 0)
                    + int(usage.get("cache_read_input_tokens", 0) or 0)
                    + int(usage.get("cache_creation_input_tokens", 0) or 0)
                )
            except Exception:
                total_tokens = 0

            # ── FAILURE branch — the turn exited non-zero (after the TR1
            #    transient-retry shield, so this is a genuine failure). ──────
            if exit_code not in (None, 0):
                if status == "done":
                    return  # already resolved; don't reopen on a late failure
                reason = summary or f"Turn failed (exit {exit_code}) with no output."
                if len(reason) > _RESOLUTION_MAX_CHARS:
                    reason = (
                        reason[:_RESOLUTION_MAX_CHARS].rstrip() + "\n\n…(truncated)"
                    )
                files, commits = _changes_since(self.paths.root, start_sha)
                fm_patch: Dict[str, Any] = {
                    "status": "blocked",
                    "failed_at": now,
                    "resolved_by": agent_id or conv,
                    "resolved_by_conv": conv,
                }
                if commits:
                    fm_patch["commit_shas"] = commits
                _patch_frontmatter(path, fm_patch)
                # No "who/when" prose prefix — the who (`resolved_by`) and
                # when (`failed_at`) live in frontmatter and the cockpit
                # renders them separately. The body carries only what's
                # actionable: WHY it failed + what it touched.
                body = f"**Failed — exit {exit_code}.**\n\n{reason}"
                facts = _resolution_facts(files, commits, total_tokens)
                if facts:
                    body += f"\n\n{facts}"
                text2 = path.read_text(errors="replace")
                path.write_text(_upsert_body_section(text2, "Resolution", body))
                self.state_manager.rebuild(broadcast=True)
                return

            # ── SUCCESS branch — only `done` tasks carry a resolution. ───────
            if not summary:
                return
            if status != "done":
                return
            # QX5 — what this turn actually changed (files + commits),
            # diffed from the HEAD captured at spawn. Empty when the turn
            # committed nothing.
            files, commits = _changes_since(self.paths.root, start_sha)
            fm_patch: Dict[str, Any] = {
                "completed_at": now,
                "resolved_by": agent_id or conv,
                "resolved_by_conv": conv,
            }
            if commits:
                fm_patch["commit_shas"] = commits  # documented v26 field
            _patch_frontmatter(path, fm_patch)
            if len(summary) > _RESOLUTION_MAX_CHARS:
                summary = summary[:_RESOLUTION_MAX_CHARS].rstrip() + "\n\n…(truncated)"
            # The resolution body is now pure signal: the agent's own
            # summary of WHAT it did, then a compact facts strip (commit +
            # files + tokens) and the changed-files list. Dropped the
            # "_Resolved by <agent> via <conv>_" prefix — the operator
            # doesn't care which ephemeral subagent ran it (that's obvious),
            # and who/when already live in frontmatter (`resolved_by`,
            # `completed_at`), which the cockpit shows on its own line.
            body = summary
            facts = _resolution_facts(files, commits, total_tokens)
            if facts:
                body += f"\n\n{facts}"
            text2 = path.read_text(errors="replace")  # re-read after fm patch
            path.write_text(_upsert_body_section(text2, "Resolution", body))
            self.state_manager.rebuild(broadcast=True)
        except Exception as e:
            _log(f"_persist_task_resolution failed for {conv}: {e}")

    def _handle_anchor_rejected(self, conv: str, reason: str, *, raw: str) -> None:
        """LAL2 stub — broadcast warning + log. LAL3 keeps this shape."""
        try:
            self.hub.broadcast(
                {
                    "type": "conv.anchor_rejected",
                    "conv": conv,
                    "reason": reason,
                    "raw_payload": raw[:512],
                    "ts": _iso_now(),
                }
            )
        except Exception as e:
            _log(f"anchor.rejected broadcast failed for {conv}: {e}")
        _log(f"anchor rejected: conv={conv} reason={reason!r}")

    def _handle_anchor_missing(self, conv: str) -> None:
        """LAL2 stub — once per turn, broadcast that the agent skipped
        the marker. Cockpit can dim the 'anchored' affordance."""
        try:
            self.hub.broadcast(
                {
                    "type": "conv.anchor_missing",
                    "conv": conv,
                    "ts": _iso_now(),
                }
            )
        except Exception as e:
            _log(f"anchor.missing broadcast failed for {conv}: {e}")

    def _broadcast_conv_activity(
        self,
        conv: str,
        *,
        live_override: Optional[bool] = None,
    ) -> None:
        """Emit a `conv.activity` WS event for one conv so cockpits
        update their live/coordinating/waiting_on flags without a
        snapshot refetch. Cheap: computes the single entry inline.

        `live_override` lets the caller force the `live` flag when
        ChatSessions hasn't yet popped the conv from `_s` (the runner's
        emit-final path races with ChatSessions._wait's pop). Pass
        `False` from the wake hook when we know the child has just
        finalised; pass `None` (default) elsewhere to read the truth
        from `chat_sessions.list_active()`.

        Called from the points that change a conv's runtime state:
            • ChatRunner spawn (live=true)
            • Wake hook on child final (live=false override)
            • chat_cancel (live=false)
        Idempotent — duplicate fires are safe; the cockpit reducer
        dedupes on conv+live+coordinating identity."""
        try:
            all_meta = self._conv_meta_load()
            live = set(self.chat_sessions.list_active())
            if live_override is False:
                live.discard(conv)
            elif live_override is True:
                live.add(conv)
            kids = []
            for c in live:
                p = (all_meta.get(c) or {}).get("parent_conv")
                if p == conv:
                    kids.append(c)
            is_live = conv in live
            m = all_meta.get(conv) or {}
            self.hub.broadcast(
                {
                    "type": "conv.activity",
                    "conv": conv,
                    "agent_type": _agent_type_normalised(
                        _agent_type_from_conv_slug(conv) or m.get("agent_type")
                    ),
                    "agent_id": m.get("agent_id"),
                    "parent_conv": m.get("parent_conv"),
                    "initiative_id": m.get("initiative_id"),
                    "task_id": m.get("task_id"),
                    "live": is_live,
                    "coordinating": (not is_live) and bool(kids),
                    "waiting_on": sorted(kids),
                    "ts": _iso_now(),
                }
            )
        except Exception as e:
            _log(f"conv.activity broadcast failed for {conv}: {e}")
