"""Prompt composition for one agent turn — the briefing pipeline.

DM-modularize-2 (py-1.14.4): lifted verbatim from daemon.py. Contains
everything that turns a cluster's filesystem state + the operator's
message into the markdown prompt piped to `claude -p`:

* ``AGENT_PROMPTS``           — declarative per-agent-type registry
                                (role / focus / redirect / storage / quota).
* ``_agent_manifest`` +
  ``_agent_type_normalised`` +
  ``_agent_type_from_conv_slug`` — agent-type resolution helpers.
* ``ProjectState``            — cheap, lazy FS summary of the cluster.
* ``StateIntegrityChecker``   — orphan-module / broken-ref repair hints.
* ``_conversation_history``   — last-N-turns rolling-summary reader.
* ``BriefingPipeline``        — stacks the sections into the final prompt.

Pure, side-effect-free composition — no subprocess, no sockets, no
locks. The only inputs are a ``Paths`` + ``Cluster`` + per-turn args;
the only output is a string. That isolation is why it extracts cleanly.

Bundler note: imports its shared helpers from utils/paths (stripped by
``bundle.py``; the names resolve in the bundle's flat namespace).
daemon.py re-exports ``AGENT_PROMPTS`` / ``_agent_manifest`` /
``_agent_type_*`` / ``BriefingPipeline`` so ``daemon.X`` stays stable
for callers and tests. quota.py imports ``AGENT_PROMPTS`` +
``_agent_manifest`` from here directly."""

from __future__ import annotations

import json
import re
from typing import Any, Dict, List, Optional, Tuple

from agent_prompts import AGENT_PROMPTS
from paths import Paths
from utils import (
    _daemon_base_url,
    _iter_timeline_files,
    _read_timeline_file,
    parse_frontmatter,
)

# The daemon's ``Cluster`` type lives in daemon.py (loaded after this module
# + after the bundle's prompts section), so it can't be imported here. Like
# storage.py, prompts annotates cluster params as ``Any`` — the only access
# is duck-typed (``cluster.data``). A TYPE_CHECKING import is NOT used: the
# bundler strips sibling/daemon imports, which would leave an empty
# ``if TYPE_CHECKING:`` block and break the single-file build.


def _conversation_history(
    paths: "Paths",
    conv: str,
    limit: int = 12,
    rolling_summary_threshold: int = 12,
    summary_head_chars: int = 200,
) -> List[str]:
    """Walk timeline files newest→oldest, return last `limit` turns of
    `conv` formatted as 'USER: …' / 'YOU (last turn): …'.

    py-1.5.0 — Rolling-summary compaction. If the conv has more than
    `rolling_summary_threshold` turns total, the older turns (beyond
    the most-recent `limit`) are collapsed into a single 'EARLIER:'
    block listing one truncated line per turn so the agent still has
    *some* awareness of what was discussed before its recent window,
    without paying the full token cost. Previous behaviour: silently
    drop everything beyond turn 12, the agent had amnesia past that.
    """
    if not paths.timeline_dir.exists():
        return []
    # Collect ALL turns for the conv, oldest → newest, scanning all
    # timeline files (jsonl + jsonl.gz from rotation). Bounded by the
    # caller's overall history dataset size; cheap on small projects.
    all_turns: List[Tuple[str, str]] = []
    for f in sorted(_iter_timeline_files(paths)):
        for ev in _read_timeline_file(f):
            if ev.get("conv") != conv:
                continue
            t = ev.get("type")
            if t not in ("chat.user", "chat.assistant", "chat.assistant.final"):
                continue
            who = "USER" if t == "chat.user" else "YOU (last turn)"
            text = str(ev.get("text") or "").strip()
            if not text:
                continue
            all_turns.append((who, text))
    if not all_turns:
        return []
    # Split into "earlier" (everything beyond `limit`) and "recent".
    if len(all_turns) <= max(limit, rolling_summary_threshold):
        recent = all_turns
        earlier: List[Tuple[str, str]] = []
    else:
        recent = all_turns[-limit:]
        earlier = all_turns[:-limit]
    out: List[str] = []
    if earlier:
        # Collapsed view of older turns — one short line each, prefixed
        # so the agent knows these are summarised.
        head_lines = [
            f"  • {w}: {t[:summary_head_chars]}{'…' if len(t) > summary_head_chars else ''}"
            for w, t in earlier
        ]
        out.append(
            f"EARLIER turns in this conversation ({len(earlier)} compacted, oldest first):"
        )
        out.extend(head_lines)
        out.append("")  # blank line before recent block
    # Recent turns at full 800-char truncation (same as before).
    out.extend(f"{w}: {t[:800]}" for w, t in recent)
    return out


# ───────────────────────────────────────────────────────────────────────
# Briefing pipeline (py-1.4.0)
#
# The agent's prompt is composed by stacking small, independent sections.
# Each section is a method on BriefingPipeline returning a markdown block
# (or "" to skip itself). Two read-only helpers feed it:
#
#   • ProjectState         — cheap FS summary (counts, emptiness)
#   • StateIntegrityChecker — orphan-module / broken-ref detection
#
# Adding a new section is a one-line append in `build()`. Each section
# is small enough to maintain without touching others, which makes
# evolution safe even as the briefing grows.
#
# Sections, in order:
#   1. role               — who you are + where
#   2. core_rules         — stable hard rules (don't push, don't edit creds)
#   3. cluster_snapshot   — N initiatives, M tasks, P modules
#   4. project_mode       — bootstrap brief if empty, ø otherwise
#   5. integrity          — orphan modules + other repair hints
#   6. cockpit_context    — operator-attached context_docs[] from /chat/dispatch
#   7. history            — last N turns from .meshkore/timeline/
#   8. user_turn          — what the user just typed
#
# All sections are separated by `\n\n---\n\n` so the LLM reads them as
# discrete blocks rather than one flat wall.


# py-1.4.1 — Stopwords for the context-coverage heuristic. These are
# tokens that pass the capitalised-token regex but are uninformative
# (sentence starters, generic acronyms). Lowercased for comparison.
_COVERAGE_STOPWORDS: set = {
    # generic English
    "this",
    "that",
    "they",
    "them",
    "their",
    "these",
    "those",
    "with",
    "without",
    "from",
    "into",
    "onto",
    "upon",
    "until",
    "after",
    "before",
    "between",
    "while",
    "during",
    "and",
    "but",
    "for",
    "not",
    "yes",
    "now",
    "next",
    "plus",
    "any",
    "all",
    "every",
    "some",
    "either",
    "neither",
    "both",
    "should",
    "would",
    "could",
    "must",
    "might",
    "will",
    "shall",
    "when",
    "where",
    "what",
    "which",
    "while",
    "whose",
    "section",
    "schema",
    "phase",
    "rule",
    "rules",
    "step",
    "steps",
    "task",
    "tasks",
    "module",
    "modules",
    "user",
    "users",
    "name",
    "kind",
    "type",
    "data",
    "code",
    "file",
    "files",
    "page",
    "site",
    "team",
    "work",
    "doing",
    # acronyms / common labels that misfire
    "mvp",
    "tba",
    "tbd",
    "etc",
    "eta",
    "etl",
    "faq",
    "kpi",
    "ai",
    "eu",
    "us",
    "uk",
    "utc",
    "url",
    "http",
    "https",
    "api",
    "ui",
    "ux",
    "ci",
    "cd",
    "qa",
    "io",
    # MeshKore-isms that shouldn't be flagged
    "meshkore",
    "cockpit",
    "architect",
    "operator",
}


class ProjectState:
    """Cheap, lazy filesystem summary of a cluster. Computed once per
    briefing build; reused across sections. Never raises on missing
    directories — empty answers everywhere instead."""

    def __init__(self, paths: "Paths"):
        self.paths = paths
        self._initiative_files: Optional[List[Any]] = None
        self._task_files: Optional[List[Any]] = None
        self._module_dirs: Optional[List[Any]] = None

    def initiative_files(self) -> List[Any]:
        if self._initiative_files is None:
            ini = self.paths.initiatives
            self._initiative_files = (
                [f for f in ini.glob("*.md") if not f.name.startswith("_")]
                if ini.exists()
                else []
            )
        return self._initiative_files

    def task_files(self, *, include_boilerplate: bool = False) -> List[Any]:
        if self._task_files is None:
            out: List[Any] = []
            md_root = self.paths.modules_dir
            if md_root.exists():
                for mdir in md_root.iterdir():
                    if not mdir.is_dir():
                        continue
                    tasks_dir = mdir / "tasks"
                    if not tasks_dir.exists():
                        continue
                    for t in tasks_dir.rglob("*.md"):
                        if t.name.startswith("_"):
                            continue
                        if not include_boilerplate and t.name.lower().startswith(
                            "t1-hello"
                        ):
                            continue
                        out.append(t)
            self._task_files = out
        return self._task_files

    def module_dirs(self) -> List[Any]:
        if self._module_dirs is None:
            md_root = self.paths.modules_dir
            self._module_dirs = (
                [m for m in md_root.iterdir() if m.is_dir()] if md_root.exists() else []
            )
        return self._module_dirs

    def is_empty(self) -> bool:
        return not self.initiative_files() and not self.task_files()


class StateIntegrityChecker:
    """Walks the cluster looking for inconsistencies that should be
    surfaced to the agent for repair on its next turn. Surfaces hints,
    not blockers — the agent decides whether to fix them now or later.

    Cheap (single FS walk + a YAML parse). Runs on every briefing.
    """

    def __init__(self, paths: "Paths", cluster: Any, project: ProjectState):
        self.paths = paths
        self.cluster = cluster
        self.project = project

    def check(self) -> List[Dict[str, Any]]:
        violations: List[Dict[str, Any]] = []
        declared_modules = {
            m.get("id")
            for m in (self.cluster.data.get("modules") or [])
            if isinstance(m, dict) and m.get("id")
        }
        # Rule: every .meshkore/modules/<X>/ should be declared in
        # cluster.yaml.modules[]. Otherwise the cockpit's module tree
        # won't show it and child tasks render as orphans.
        for mdir in self.project.module_dirs():
            mid = mdir.name
            if mid not in declared_modules:
                violations.append(
                    {
                        "kind": "module_not_declared",
                        "module_id": mid,
                        "fix": (
                            f"Append `{{id: {mid}, kind: area, name: '{mid.capitalize()}'}}`"
                            " to `.meshkore/public/cluster.yaml.modules[]` so the cockpit's"
                            " module tree shows this module + its tasks."
                        ),
                    }
                )
        # Rule: every task's `initiative:` should reference an existing
        # initiative file. Surfaces typos and renames.
        initiative_ids = {self._read_id(f) for f in self.project.initiative_files()}
        initiative_ids.discard(None)
        for tf in self.project.task_files():
            tid = self._read_id(tf)
            ini = self._read_field(tf, "initiative")
            if ini and ini not in initiative_ids:
                violations.append(
                    {
                        "kind": "task_initiative_broken",
                        "task_id": tid or tf.name,
                        "initiative_ref": ini,
                        "fix": (
                            f"Task `{tid or tf.name}` references initiative"
                            f" `{ini}` which does not exist under"
                            " `.meshkore/roadmap/initiatives/`. Either create"
                            " the initiative file or update the task's"
                            " `initiative:` frontmatter to an existing id"
                            f" (current: {sorted(initiative_ids)})."
                        ),
                    }
                )
        # Rule: every initiative whose status is `active` or `next`
        # should have ≥1 child task. `backlog` / `done` are exempt.
        # This catches "I created an initiative and forgot the tasks".
        tasks_by_initiative: Dict[str, List[str]] = {}
        for tf in self.project.task_files():
            ini = self._read_field(tf, "initiative")
            if ini:
                tasks_by_initiative.setdefault(ini, []).append(tf.name)
        for inif in self.project.initiative_files():
            iid = self._read_id(inif)
            status = (self._read_field(inif, "status") or "").lower()
            if not iid:
                continue
            if status not in ("active", "next"):
                continue
            children = tasks_by_initiative.get(iid) or []
            if not children:
                violations.append(
                    {
                        "kind": "initiative_without_tasks",
                        "initiative_id": iid,
                        "status": status,
                        "fix": (
                            f"Initiative `{iid}` is `{status}` but has no child"
                            " tasks. Either add 1-2 scaffolding tasks (linked"
                            f" via `initiative: {iid}` in their frontmatter)"
                            " or drop the initiative back to `status: backlog`"
                            " until you're ready to populate it."
                        ),
                    }
                )
            # py-1.6.2 — Over-dense initiative. >12 active/next tasks
            # under one initiative is a roadmap anti-pattern: the cockpit
            # card becomes unscannable and the initiative is almost
            # certainly mixing multiple work-streams.
            elif len(children) > 12:
                violations.append(
                    {
                        "kind": "initiative_too_dense",
                        "initiative_id": iid,
                        "child_count": len(children),
                        "fix": (
                            f"Initiative `{iid}` carries {len(children)} child"
                            " tasks — that's almost always multiple work-streams"
                            " grouped under one card. Split into work-stream-"
                            "coherent sub-initiatives (e.g., 'Auth & identity',"
                            " 'Canvas viewer', 'Anchoring chain'), each with"
                            " 3-8 tasks. Repoint each task's `initiative:`"
                            " frontmatter at its new id. Then either repurpose"
                            f" `{iid}` as one of the new work-streams or move"
                            " its file to `.meshkore/roadmap/initiatives/log/`"
                            f" with `status: superseded` + `superseded_by:`."
                        ),
                    }
                )
        # py-1.4.1 — Context coverage gap (heuristic). Finds capitalised
        # tokens (brand / product / proper-noun-ish) mentioned ≥3 times
        # in context.md but 0 times across any task / initiative file.
        # Conservative: stopword filter + frequency floor → low false
        # positives. Surfaced as a single hint, NOT a hard violation.
        coverage_gap = self._check_context_coverage()
        if coverage_gap:
            violations.append(coverage_gap)
        # py-1.4.3 — Coverage matrix discipline.
        cov_v = self._check_coverage_doc()
        if cov_v:
            violations.append(cov_v)
        return violations

    def _check_coverage_doc(self) -> Optional[Dict[str, Any]]:
        """Once the cluster has at least one initiative, enforce that
        `.meshkore/docs/coverage.md` exists and has no `?` / `TBD` /
        `TODO` / `FIXME` placeholders in the Coverage column."""
        if not self.project.initiative_files():
            return None  # bootstrap still in progress; not yet expected
        cov_path = self.paths.docs_dir / "coverage.md"
        if not cov_path.exists():
            return {
                "kind": "coverage_doc_missing",
                "fix": (
                    "Create `.meshkore/docs/coverage.md` mapping every"
                    " numbered requirement from the brief (sections + rules"
                    " + explicit deliverables) to a task id OR a"
                    " `defer: <reason>` marker. See the bootstrap brief's"
                    " 'Coverage matrix' block for the required format —"
                    " three sections: Sections, Rules, Explicit deliverables."
                ),
            }
        try:
            text = cov_path.read_text(errors="replace")
        except OSError:
            return None
        # Detect placeholders in the Coverage column of pipe-tables.
        # Matches `| ? |`, `| TBD |`, `| TODO |`, `| FIXME |`, `|  |`
        # (empty), and `|   ???  |`. Case-insensitive.
        gap_pat = re.compile(r"\|\s*(\?+|TBD|TODO|FIXME|N/A)\s*\|", re.IGNORECASE)
        gap_hits = gap_pat.findall(text)
        # Empty cells: only count those that look like a final column
        # (line ends with the empty cell pipe). Skip header/separator
        # rows ("|---|---|").
        empty_count = 0
        for line in text.splitlines():
            if line.strip().startswith("|") and "---" not in line:
                cells = [c.strip() for c in line.strip().strip("|").split("|")]
                if cells and cells[-1] == "":
                    empty_count += 1
        total_gaps = len(gap_hits) + empty_count
        if total_gaps == 0:
            return None
        return {
            "kind": "coverage_gaps_in_doc",
            "count": total_gaps,
            "fix": (
                f"`.meshkore/docs/coverage.md` has {total_gaps} row(s) with"
                " a placeholder (`?`, `TBD`, `TODO`, `FIXME`, `N/A`, or"
                " empty) in the Coverage column. Resolve each: either add"
                " the task that addresses the requirement (and reference"
                " it in the cell), or replace with `defer: <reason>`."
            ),
        }

    def _check_context_coverage(self) -> Optional[Dict[str, Any]]:
        ctx_path = self.paths.docs_dir / "context.md"
        if not ctx_path.exists():
            return None
        try:
            ctx_text = ctx_path.read_text(errors="replace")
        except OSError:
            return None
        haystack_parts: List[str] = []
        for f in (
            self.project.task_files(include_boilerplate=True)
            + self.project.initiative_files()
        ):
            try:
                haystack_parts.append(f.read_text(errors="replace"))
            except OSError:
                pass
        haystack_lower = "\n".join(haystack_parts).lower()

        # Capitalised tokens, 4+ chars, allow dot + hyphen inside (FAL.ai,
        # DALL-E, Cloudflare, SvelteKit). All-caps acronyms are caught by
        # the same regex.
        pat = re.compile(r"\b[A-Z][A-Za-z0-9.\-]{3,}\b")
        counts: Dict[str, int] = {}
        for m in pat.finditer(ctx_text):
            tok = m.group(0)
            low = tok.lower()
            if low in _COVERAGE_STOPWORDS:
                continue
            counts[tok] = counts.get(tok, 0) + 1
        # Threshold: appears ≥3 times in context AND 0 times across
        # tasks + initiatives. Top 8 by frequency.
        gaps: List[Tuple[str, int]] = []
        for tok, n in counts.items():
            if n < 3:
                continue
            if tok.lower() in haystack_lower:
                continue
            gaps.append((tok, n))
        gaps.sort(key=lambda x: (-x[1], x[0]))
        gaps = gaps[:8]
        if not gaps:
            return None
        return {
            "kind": "context_coverage_gap",
            "tokens": [{"token": t, "mentions": n} for t, n in gaps],
            "fix": (
                "These proper-noun-ish terms appear repeatedly in"
                " `.meshkore/docs/context.md` but in 0 task / initiative"
                " files. Either (a) add a task that addresses them, or"
                " (b) write a `> defer: <reason>` line in context.md so"
                " future briefings stop flagging them as gaps."
            ),
        }

    @staticmethod
    def _read_id(path: Any) -> Optional[str]:
        try:
            text = path.read_text(errors="replace")
        except OSError:
            return None
        fm = parse_frontmatter(text)
        v = fm.get("id")
        return str(v) if v else None

    @staticmethod
    def _read_field(path: Any, key: str) -> Optional[str]:
        try:
            text = path.read_text(errors="replace")
        except OSError:
            return None
        fm = parse_frontmatter(text)
        v = fm.get(key)
        return str(v) if v else None


# py-1.7.0 — Specialised agent prompt registry. Each agent type gets a
# role + focus + redirect + storage rules block. The default "custom"
# (a.k.a. General coder) keeps the original coordinator behaviour: full
# roadmap / module / task authority. Service agents (deploy / db /
# testing / audit / docs / review) get a tight focus + an explicit
# "redirect to General coder" clause so they refuse out-of-scope work
# cleanly instead of bumbling into roadmap edits.
#
# Why declarative: scaling. Adding a new agent type later = one entry
# here, no `if agent_type == 'foo':` branches scattered across the
# briefing pipeline. The pipeline reads from this dict.
def _agent_manifest(agent_type: str) -> Dict[str, str]:
    """py-1.10.27 — Per-agent platform+model manifest.

    Reads optional `platform` / `model` fields from `AGENT_PROMPTS[agent_type]`
    (falls back to claude-code/auto for any type that doesn't declare them —
    everything ships through Claude Code today, but DeepSeek / Codex / direct
    Anthropic API agents will declare their own values when wired). The
    returned `quota_key` is the persistence + pause-state key used by
    QuotaState — different agent types that share a platform+model share
    a quota pool.

    Future extension: if a single agent_type spans multiple models (e.g.
    a router that picks Claude or DeepSeek per turn), this returns the
    DEFAULT entry; the dispatch path can override per-turn."""
    p = AGENT_PROMPTS.get(agent_type) or AGENT_PROMPTS.get("custom") or {}
    platform = str(p.get("platform") or "claude-code")
    model = str(p.get("model") or "auto")
    return {
        "platform": platform,
        "model": model,
        "quota_key": f"{platform}/{model}",
    }


# AGENT_PROMPTS relocated to agent_prompts.py (DM-modularize-3) — ~940
# lines of prompt text; imported above and re-exported so callers that
# do `from prompts import AGENT_PROMPTS` (quota.py, daemon.py) are unchanged.


def _agent_type_normalised(t: Optional[str]) -> str:
    """Return a known agent_type, defaulting to 'custom' if missing/unknown."""
    if not t:
        return "custom"
    t = str(t).strip().lower()
    return t if t in AGENT_PROMPTS else "custom"


def _agent_type_from_conv_slug(conv: str) -> Optional[str]:
    """py-1.10.12 — Infer agent_type from the conv slug pattern.

    The cockpit's `createConv({type: 'roadmap-architect'})` produces
    slugs of shape `roadmap-architect-<5chars>`. The slug is the only
    UNFORGEABLE signal of intent — every other channel (body field,
    conv_meta sidecar, cockpit localStorage) can drift out of sync.

    When the slug carries the type, we treat it as the source of truth
    and force the agent_type to match. Protects against:
      - cockpit JS stuck on a stale bundle that drops `agent_type`
        from the dispatch body
      - cockpit localStorage convMeta that pre-dates an agent type
        being added to the AgentType union
      - sidecar entries written by an older daemon that defaulted
        to 'custom' before the type was registered

    Returns None for slugs with no implied type."""
    if not conv:
        return None
    for prefix, implied in (
        ("roadmap-architect-", "roadmap-architect"),
        ("deploy-", "deploy"),
        ("db-", "db"),
        ("testing-", "testing"),
        ("audit-", "audit"),
        ("docs-", "docs"),
        ("review-", "review"),
    ):
        if conv.startswith(prefix) and implied in AGENT_PROMPTS:
            return implied
    return None


class BriefingPipeline:
    """Composes the prompt sent to `claude -p` for one agent turn.
    See module-level comment above for the section order + rationale."""

    SECTION_SEP = "\n\n---\n\n"

    def __init__(
        self,
        *,
        paths: "Paths",
        cluster: Any,
        identity: str,
        conv: str,
        user_text: str,
        context_docs: Optional[List[Dict[str, Any]]] = None,
        agent_type: Optional[str] = None,
        agent_id: Optional[str] = None,
    ):
        self.paths = paths
        self.cluster = cluster
        self.identity = identity
        self.conv = conv
        self.user_text = user_text
        self.context_docs = context_docs or []
        # py-1.7.0 — agent_type drives role / focus / redirect / rules
        # selection from AGENT_PROMPTS. Defaults to 'custom' (General
        # coder) when missing/unknown so older cockpits and direct API
        # callers keep working.
        self.agent_type = _agent_type_normalised(agent_type)
        self.agent_id = (agent_id or "").strip() or None
        self.project = ProjectState(paths)
        self.integrity = StateIntegrityChecker(paths, cluster, self.project)
        # py-1.7.0 — cadence: detect whether this conv has had any prior
        # assistant turn. The full role+rules block is sent on the first
        # turn (so the agent gets the complete onboarding); on subsequent
        # turns we send a tight role reminder only, saving tokens and
        # keeping the conversation snappier.
        self.is_first_turn = self._detect_first_turn()

    def _detect_first_turn(self) -> bool:
        try:
            for f in _iter_timeline_files(self.paths):
                for ev in _read_timeline_file(f):
                    if ev.get("conv") != self.conv:
                        continue
                    if ev.get("type") in (
                        "chat.assistant.final",
                        "chat.assistant.delta",
                    ):
                        return False
            return True
        except Exception:
            return True

    def build(self) -> str:
        sections = [
            # Standard §3.5 (v25 hard_rule) — invariant project context,
            # prepended BEFORE the role block. Empty when the cluster has
            # no .meshkore/context/ tree yet.
            self._section_project_context(),
            self._section_role(),
            self._section_core_rules(),
            self._section_agent_focus(),
            self._section_agent_redirect(),
            self._section_agent_memory(),
            self._section_cluster_snapshot(),
            self._section_project_mode(),
            self._section_integrity(),
            self._section_cockpit_context(),
            self._section_history(),
            # py-1.10.8 — only non-empty when user_text starts with
            # `[architect-consult]` on the `_onboarding_v1` conv. Forces
            # A001 to decide instead of bouncing the question back.
            self._section_consult_addendum(),
            self._section_user_turn(),
        ]
        brief = self.SECTION_SEP.join(s for s in sections if s and s.strip())
        # Substitute the live daemon version into the architect's commit-
        # trailer SOP. DAEMON_VERSION lives in daemon.py (loaded last in the
        # bundle); imported here at call time so the AGENT_PROMPTS dict can be
        # built at module load without it (DM-modularize-2). No-op for agent
        # types whose briefing doesn't carry the placeholder.
        from daemon import DAEMON_VERSION

        return brief.replace("__MESHKORE_VERSION__", DAEMON_VERSION)

    # ── sections ──────────────────────────────────────────────────

    def _section_project_context(self) -> str:
        """Standard §3.5 (v25 hard_rule) — serialize `.meshkore/context/`
        as the invariant PROJECT CONTEXT block, prepended before the role
        on every spawn. Order + markers per `context.serialization_to_agent`.
        Returns "" when the cluster has no context/ tree yet (a fresh
        cluster) — the legacy `docs/context.md` pointers in other sections
        still apply until the Roadmap Author bootstraps context/."""
        root = self.paths.context_dir
        if not root.is_dir():
            return ""

        def body_of(p: "Any") -> str:
            try:
                text = p.read_text(encoding="utf-8", errors="replace")
            except OSError:
                return ""
            # Strip YAML frontmatter (only when both fences are present).
            if text.startswith("---"):
                end = text.find("\n---", 3)
                if end != -1:
                    nl = text.find("\n", end + 1)
                    if nl != -1:
                        text = text[nl + 1 :]
            return text.strip()

        parts: List[str] = []

        def add(marker: str, p: "Any") -> None:
            if p.is_file():
                b = body_of(p)
                if b:
                    parts.append(f"{marker}\n{b}")

        add("=== PROJECT CONTEXT ===", root / "overview.md")
        add("=== PRODUCT ===", root / "product.md")
        add("=== STACK ===", root / "stack.md")
        add("=== ARCHITECTURE ===", root / "architecture.md")
        add("=== CONSTRAINTS ===", root / "constraints.md")
        add("=== GLOSSARY ===", root / "glossary.md")

        def folder_chunk(folder: "Any", *, newest_first: bool) -> str:
            if not folder.is_dir():
                return ""
            entries = [f for f in folder.glob("*.md") if f.name != "README.md"]
            entries.sort(reverse=newest_first)  # filenames are date- or slug-sorted
            chunk: List[str] = []
            readme = folder / "README.md"
            if readme.is_file():
                b = body_of(readme)
                if b:
                    chunk.append(b)
            for f in entries:
                b = body_of(f)
                if b:
                    chunk.append(b)
            return "\n\n".join(chunk)

        dec = folder_chunk(root / "decisions", newest_first=True)
        if dec:
            parts.append("=== DECISIONS (newest first) ===\n" + dec)
        crit = folder_chunk(root / "criteria", newest_first=False)
        if crit:
            parts.append("=== CRITERIA ===\n" + crit)

        if not parts:
            return ""

        block = "\n\n".join(parts)
        # Defensive cap (~ the §3.5 3000w/4500tok budget) so a runaway
        # context/ can't blow the prompt. Dispatch-time budget refusal is
        # a separate gate; here we just bound the briefing.
        CAP = 24000
        truncated = ""
        if len(block) > CAP:
            block = block[:CAP]
            truncated = (
                "\n…[context truncated — over the §3.5 4500-token budget; "
                "trim .meshkore/context/]"
            )

        header = (
            "Everything between the markers below is the project's INVARIANT "
            "context (MeshKore Standard §3.5). Treat it as authoritative and "
            "unchanging — do not re-derive or re-debate it.\n"
        )
        return f"{header}\n{block}{truncated}\n\n=== END CONTEXT ==="

    def _section_role(self) -> str:
        # py-1.7.0 — Role text is now driven by AGENT_PROMPTS so service
        # agents (deploy / db / testing / ...) get their own framing,
        # not a generic "coordinator" label.
        prompt = AGENT_PROMPTS.get(self.agent_type) or AGENT_PROMPTS["custom"]
        role = prompt["role"]
        # On subsequent turns, send a tight role reminder only.
        if not self.is_first_turn:
            return (
                f"## Role reminder\n\n{role}\n\n"
                f"Cluster root: `{self.paths.root}` · Identity: "
                f"`{self.identity}` · Conv: `{self.conv}`"
                + (f" · Agent: `{self.agent_id}`" if self.agent_id else "")
            )
        return (
            f"## Role\n\n{role}\n\n"
            f"Cluster root: `{self.paths.root}`\nIdentity: `{self.identity}`"
            f" · Conv: `{self.conv}`"
            + (f" · Agent: `{self.agent_id}`" if self.agent_id else "")
        )

    def _section_core_rules(self) -> str:
        try:
            port = int(self.paths.port_file.read_text().strip())
        except (OSError, ValueError):
            port = 5570
        # py-1.10.14 — single source of truth for the in-prompt base URL.
        # HTTPS over `daemon.meshkore.com:<port>` when the TLS bundle is
        # present (the default since D-TLS-01), plain HTTP only as
        # back-compat. Plain `http://localhost:<port>` against a
        # TLS-wrapped socket returns RST and breaks every spawned agent.
        base = _daemon_base_url(port)

        # py-1.7.0 — Universal rules: every agent type sees these every
        # turn. Short, load-bearing. These are NOT role-specific.
        universal = [
            "## Universal rules (every agent, every turn)",
            "",
            "- Don't push to git unless the user explicitly asks.",
            f"- Don't invent version numbers; ask `POST {base}/version/next`.",
            "- Never edit `.meshkore/credentials/`, `.meshkore/.runtime/` or generated `state.json`.",
            "- The cockpit auto-refreshes ~2s after any write under `.meshkore/` — don't tell the user to reload.",
            "- Reply concisely — follow the **Output contract** below. The portal renders your stdout as the chat answer.",
            "- **Mention initiatives and tasks by their `#<id>` in chat output** (Standard §22, v20+). When you add, remove, rename, defer, or otherwise touch a roadmap item, the operator-facing line MUST start with — or contain — `#<id>`. Example: `✓ added #I18 task #T-vote-API`, `✗ removed #T-fixture-loader from #I19`, `↻ split #I21 into #I21 + #I27`. This lets the operator click-locate the item in the roadmap UI; bare titles in chat are not enough.",
            "- **Anchor every turn to (initiative, task)** (Standard §24, v23+; `anchor.v1` wire protocol, py-1.12.31+). The FIRST line of EVERY assistant reply MUST be a structured anchor marker — the daemon parses it, persists to `conv_meta`, creates files if needed, and STRIPS the line from the output the user sees. Four valid shapes:",
            '    `⟦anchor⟧ {"i":"<init_id>","t":"<task_id>"}`                              — both exist; resolve + persist',
            '    `⟦anchor⟧ {"new_i":{"id","title","oneliner","modules":[...]},"new_t":{"id","title","category"}}`  — create both',
            '    `⟦anchor⟧ {"new_t":{"id","title","category","initiative":"<existing>"}}`  — task in existing init',
            '    `⟦anchor⟧ {"info":true}`                                                  — informational turn (no anchor)',
            '  Mid-turn task transitions: emit `⟦anchor-progress⟧ {"t":"<task_id>","status":"done"}` when you finish a task; the daemon writes `status: done` to the .md file. Then emit a NEW `⟦anchor⟧` if you\'re starting the next task in the same turn.',
            '  Frontmatter contracts the daemon enforces: initiative slug `^[a-z][a-z0-9-]{1,31}$`; task id `^[A-Za-z][A-Za-z0-9_-]{1,31}$`; exactly one module per task (Standard §4). The newly-created files land at the top of the cockpit\'s roadmap timeline with a ✨ NEW badge — that IS the chronology of "what is happening right now". Full recipe + worked examples: `.meshkore/docs/conventions/initiative-anchored-execution.md`.',
            '  **`i` is the initiative\'s lowercase SLUG (its file-stem `id`, exactly as it appears in the cluster state) — NOT the `#`-prefixed display id you use in chat.** Never build a slug from the display number. ✗ `{"i":"I32-model-v9-points-only"}` (that\'s the `#I32` display id glued to a title). ✓ `{"i":"model-v9-points-only"}` (the real file-stem). If you can see an initiative as `#I32` but don\'t know its slug, read `.meshkore/roadmap/initiatives/` and copy the actual filename stem. To CREATE a new initiative use `new_i` — do not invent an `i` for one that doesn\'t exist on disk.',
            '  Decision chain when conv_meta is empty: (a) read `.meshkore/roadmap/initiatives/`; if one matches the operator\'s intent by title+oneliner+module → emit `⟦anchor⟧ {"i":"<match>","t":"<existing-task>"}`; (b) if no clear match, CREATE a new initiative + 1-3 tasks via `⟦anchor⟧ {"new_i":{...},"new_t":{...}}` (pick a slug derived from the operator\'s request; modules from the area you\'ll touch); (c) if the user\'s question is purely informational (e.g. "¿qué versión del daemon?"), emit `⟦anchor⟧ {"info":true}` instead.',
            "",
            "## Output contract — how your chat answer is shaped (EVERY agent, EVERY turn)",
            "",
            "The operator reads your stdout on a chat wall and decides from a 5-second scan — NOT a full read. This contract OVERRIDES any urge to be thorough in the visible answer. Thoroughness goes inside collapsible blocks, never at the top level.",
            "",
            "1. **Lead with a ≤8-line summary.** Answer the actual question first: what you found / what you'll do, which files or areas you touch, and the plan as N steps. No preamble, no restating the request, no 'I'll now…'.",
            "2. **All detail lives inside `<details>` blocks** — one per file or topic. Anything beyond the summary (per-file findings, SQL/code specifics, legacy-field lists, audit results, rationale) MUST be wrapped so the operator expands only what they care about. The cockpit renders `<details>` as a native click-to-expand block:",
            "       <details><summary>apps/api/src/pieces/list.rs — 3 changes</summary>",
            "",
            "       - reads `?cx&cy&r`, adds bbox filter, caps `LIMIT 1000`",
            "       - drops legacy `Tile` fields (cx/cy/span/tint/…)",
            "       </details>",
            "   The `<summary>` is a ONE-LINE headline: `<file> — <N changes>` or `<topic> — <verdict>`. Leave a BLANK LINE after the `</summary>`'s line so the markdown inside (lists, code fences) renders.",
            "3. **No detail prose outside a `<details>`.** If a paragraph or bullet list isn't part of the ≤8-line summary, it belongs in a details block. Never dump a wall of bullets at the top level.",
            "4. **Don't narrate process** ('I read X, then checked Y…'). State conclusions; the operator asks for steps if they want them.",
            "5. When unsure, go SHORTER. The operator can click to expand or ask a follow-up — they cannot un-read 50 lines. A reply that needs scrolling has failed this contract.",
            "",
            "Full rationale + worked before/after: `.meshkore/docs/conventions/output-contract.md`.",
            "",
            "## MeshKore standard (where things live)",
            "",
            "- `.meshkore/` — everything the cluster knows lives here. The operator never edits it by hand; you do.",
            "- `.meshkore/modules/<id>/` — module-scoped work. Tasks live at `.meshkore/modules/<id>/tasks/*.md`.",
            "- `.meshkore/roadmap/initiatives/*.md` — initiatives (work-streams). Status: `active` / `next` / `backlog` / `done`.",
            "- `.meshkore/log/<UTC-date>.md` — daily activity log (diary). **One short paragraph per relevant event** (1–4 sentences, ≤ 1 200 chars). NEVER paste full diffs, full task lists, full file dumps — point at the artifact (`commit <sha>`, `task <id>`) and summarise the outcome. The diary must stay readable end-to-end; a turn that mutates ≥3 files writes ONE summary line, not one per file.",
            "- `.meshkore/docs/coverage.md` — coverage matrix (requirement → which task delivers it).",
            "- `.meshkore/agents/_types/<agent-type>/memory.md` — your role's long-term memory (see below).",
            "",
            "## Daemon endpoints you should know",
            "",
            f"- Base URL: `{base}` (use exactly this — the loopback listener uses TLS; plain `http://localhost:<port>` is reset by the socket).",
            f"- `POST {base}/version/next` — get the next valid version for a key (never invent numbers).",
            f"- `POST {base}/log/append` (or just append to `.meshkore/log/<UTC-date>.md` directly) — operator activity log.",
            f"- `GET  {base}/state` — current cluster state (initiatives, tasks, modules, integrity flags).",
            f"- `POST {base}/chat/dispatch` — used by the cockpit; you receive your prompt via this path, you don't call it.",
            f"- `GET  {base}/debug/tail?last=<secs>&tag=<csv>&level=<min>` — structured JSONL of everything that just happened (chat-dispatch, architect-wake, subagent-final, init-archive, http, cockpit logs). 30-min rolling window. Read this BEFORE asking the operator anything — most bugs reveal themselves here. See `.meshkore/docs/conventions/debug-stream.md`.",
            "- Privileged endpoints (`/chat/dispatch`, `/version/next`, `/log/append`, `/runs`, …) require `Authorization: Bearer <portal-token>`; the token lives at `.meshkore/credentials/portal-token`. `/health` and `/state` are open.",
            "- If a request fails with `Connection reset by peer` or `Recv failure`, you're talking to the TLS socket over plain HTTP — switch the scheme to `https://` and retry. This is NOT a daemon outage.",
            "",
            "## How to flag persistent learnings",
            "",
            "- When you discover something other agents of your role would want to know next time (a credential location, a flaky test pattern, a migration gotcha), end your reply with a line: `REMEMBER: <one short fact>`.",
            "- The daemon harvests `REMEMBER:` lines and appends them to your role's `memory.md`. Don't write to that file directly.",
            "",
            "Reference docs:",
            "  - https://meshkore.com/standard.json — canonical schemas",
            "  - https://meshkore.com/cluster/operate — operator manual",
            "  - `.meshkore/docs/context.md` — project-specific context (if present)",
            "  - `.meshkore/docs/conventions/*.md` — repo conventions",
        ]

        # General coder ('custom') additionally owns the roadmap, so it
        # gets the granularity rules. Service agents don't.
        if self.agent_type == "custom":
            general_coder_extras = [
                "",
                "## Module / task / initiative authority (General coder only)",
                "",
                "- When you create a new module directory `.meshkore/modules/<id>/`, ALSO add `{id: <id>, kind: area, name: '<Title>'}` to `cluster.yaml.modules[]`.",
                "- Every initiative you mark `active` or `next` must have ≥1 child task linked via `initiative: <id>` in the task's frontmatter. Use `status: backlog` for placeholders.",
                "",
                "### Task granularity",
                "",
                "- Target grain: **one task ≈ one week of focused work**.",
                "- If a candidate task would take > 2 weeks to deliver, split it (with `depends_on:` chains).",
                "- If a candidate task would take < 2 days, fold it into a sibling or the parent task's body.",
                "- Every task body MUST end with a `## Done when` section listing 2-5 concrete acceptance criteria the operator can verify without asking you.",
                "",
                "### Initiative granularity",
                "",
                '- Each initiative = **ONE coherent work-stream**, never a phase or release name. ✓ "Auth & identity", "Payments & credits". ✗ "MVP", "Phase 1", "Closed beta".',
                "- Target shape: **3-8 child tasks** in `active` / `next` status.",
                "- **Hard limit: never > 12 active/next tasks** per initiative. The integrity check (next turn's briefing) flags over-dense initiatives.",
                "- **Lower limit: ≥ 2 child tasks** for any active/next initiative. If only 1 task fits — fold, or drop the initiative back to `backlog`.",
                "- When SPLITTING an initiative: create the new files first, re-point each child task's `initiative:` frontmatter, then move the old file to `.meshkore/roadmap/initiatives/log/<old-id>.md` with `status: superseded` + `superseded_by:`.",
                "- An initiative's `## Done when` is the WORK-STREAM completion signal, verifiable independently.",
                "",
                "### Coverage matrix",
                "",
                "- When you create or modify any task / initiative, update `.meshkore/docs/coverage.md` to reflect it. Create the file if missing.",
            ]
            return "\n".join(universal + general_coder_extras)
        return "\n".join(universal)

    def _section_agent_focus(self) -> str:
        # py-1.7.0 — Service agents get their narrow focus block. The
        # General coder doesn't (it has no narrowing focus — its scope
        # is the whole cluster).
        prompt = AGENT_PROMPTS.get(self.agent_type) or AGENT_PROMPTS["custom"]
        focus = prompt.get("focus") or ""
        return focus.strip()

    def _section_agent_redirect(self) -> str:
        # py-1.7.0 — Out-of-scope policy for service agents. General
        # coder has nothing to redirect.
        prompt = AGENT_PROMPTS.get(self.agent_type) or AGENT_PROMPTS["custom"]
        redirect = prompt.get("redirect") or ""
        if not redirect.strip():
            return ""
        return "## Out-of-scope policy\n\n" + redirect.strip()

    def _section_agent_memory(self) -> str:
        # py-1.7.0 — Per-type long-term memory at
        # `.meshkore/agents/_types/<agent-type>/memory.md`. Populated by
        # the daemon when the agent ends a turn with `REMEMBER: …`
        # lines. Shared across all conversations of the same role.
        try:
            mem_path = self.paths.agents_dir / "_types" / self.agent_type / "memory.md"
            if not mem_path.exists():
                return ""
            txt = mem_path.read_text(errors="replace").strip()
            if not txt:
                return ""
            # Cap to ~4 KB so this section never dominates the briefing.
            if len(txt) > 4096:
                txt = txt[-4096:]
                # Trim to start of next line so we don't cut mid-entry.
                nl = txt.find("\n")
                if nl > 0:
                    txt = txt[nl + 1 :]
            return (
                f"## Your role's accumulated memory "
                f"(`agents/_types/{self.agent_type}/memory.md`)\n\n"
                f"{txt}\n\n"
                "These are facts past instances of your role have flagged "
                "as worth remembering. Use them; don't repeat them back."
            )
        except Exception:
            return ""

    def _section_cluster_snapshot(self) -> str:
        n_ini = len(self.project.initiative_files())
        n_tasks = len(self.project.task_files())
        declared_mods = self.cluster.data.get("modules") or []
        n_decl_mods = len(declared_mods) if isinstance(declared_mods, list) else 0
        n_dir_mods = len(self.project.module_dirs())
        bits = [
            f"- {n_ini} initiative(s) at `.meshkore/roadmap/initiatives/`",
            f"- {n_tasks} task(s) across modules (excluding the wizard's T1-hello boilerplate)",
            f"- {n_decl_mods} module(s) declared in `cluster.yaml.modules[]`",
        ]
        if n_dir_mods != n_decl_mods:
            bits.append(
                f"- {n_dir_mods} module directory(ies) on disk — mismatch with declared"
                " (see Integrity section below)"
            )
        return "## Cluster snapshot\n\n" + "\n".join(bits)

    def _section_project_mode(self) -> str:
        if not self.project.is_empty():
            return ""
        # py-1.4.3 — Scale the target task count by brief size. Briefs
        # in the kilobytes deserve more granular task decomposition than
        # "build a todo list" one-liners. Sources of brief size,
        # in order of preference: context.md (already written),
        # accumulated chat.user texts in this conv, the current
        # user_text. The number is heuristic, not enforced — the agent
        # picks a sensible point inside the range.
        brief_chars = self._estimate_brief_size()
        if brief_chars < 500:
            ini_range, task_range, breadth = "1-2", "3-8", "tiny"
        elif brief_chars < 2000:
            ini_range, task_range, breadth = "2-4", "8-15", "small"
        elif brief_chars < 5000:
            ini_range, task_range, breadth = "3-5", "15-25", "medium"
        elif brief_chars < 10000:
            ini_range, task_range, breadth = "3-6", "25-40", "large"
        else:
            ini_range, task_range, breadth = "4-8", "40-60", "comprehensive"
        return "\n".join(
            [
                "## Project mode: BOOTSTRAPPING (empty cluster)",
                "",
                f"The cluster at `{self.paths.root}` has 0 initiatives + 0 real",
                "tasks. Your purpose right now is to bootstrap the roadmap,",
                "not to interrogate the user until you have a perfect brief.",
                "",
                "**Write FIRST, talk SECOND.** As soon as the user has given",
                "you ANY substantive description of the project — its goal,",
                "audience, rough scope, any constraint — STOP asking",
                "clarifying questions and write:",
                "",
                f"### Brief size: ≈ {brief_chars} chars → {breadth} scope",
                "",
                f"  - **{ini_range} initiatives** at `.meshkore/roadmap/initiatives/<id>.md`",
                "    (frontmatter per `initiative` schema). Each initiative is a",
                '    **coherent work-stream**, named by what it builds: "Auth &',
                '    identity", "Canvas viewer", "Anchoring chain", "Payments",',
                '    "Observability". NEVER name initiatives by phase ("MVP",',
                '    "Phase 1", "Closed beta") — those collapse into one giant',
                "    catch-all card and break the roadmap UX. Target 3-8 child",
                "    tasks per initiative; hard limit 12. The next-turn integrity",
                "    check flags initiatives that exceed that.",
                f"  - **{task_range} initial tasks** distributed across modules",
                "    under `.meshkore/modules/<module>/tasks/<id>.md`. Bias",
                "    towards MORE tasks if the brief is long — every numbered",
                "    section, every rule, every explicit deliverable that's",
                "    in scope for Phase 1 should map to either a task or an",
                "    explicit `defer: <reason>` marker in coverage.md (see",
                "    below). Each task ≈ one week of focused work; split",
                "    tasks that exceed two weeks; fold tasks under two days.",
                "    Module directories MUST be declared in `cluster.yaml.modules[]`",
                "    on creation (otherwise the cockpit tree won't show them).",
                "  - A short `.meshkore/docs/context.md` capturing goal,",
                "    audience, constraints, and non-obvious decisions from the",
                "    brief. Frontmatter per `doc_frontmatter`.",
                "",
                "### Coverage matrix (mandatory deliverable)",
                "",
                "Write `.meshkore/docs/coverage.md` mapping EVERY numbered",
                "section, EVERY rule, and EVERY explicit deliverable in the",
                "user's brief to a task id OR a `defer: <reason>` marker.",
                "This is what makes the roadmap auditable — without it, gaps",
                "stay invisible until someone notices a feature wasn't built.",
                "Required shape (3 sections, in order):",
                "",
                "```markdown",
                "---",
                "title: Coverage matrix",
                "updated: YYYY-MM-DD",
                "owner: <you>",
                "---",
                "",
                "# Coverage matrix — `<cluster>`",
                "",
                "Maps every brief requirement to a task id or a deferral.",
                "Maintained on every roadmap-modifying turn.",
                "",
                "## Sections",
                "",
                "| Source | Requirement | Coverage |",
                "|---|---|---|",
                "| §4 Cosmology | Halo FIFO eviction | API7 |",
                "| §4 Cosmology | Oort decay state machine | WEB5 |",
                "| §6 Economic | Referral program | defer: Phase 4 (growth) |",
                "",
                "## Rules",
                "",
                "| # | Rule | Coverage |",
                "|---|---|---|",
                "| 1 | AI-only generation | AI1 |",
                "| 10 | Named zones | defer: Phase 5 (B2B) |",
                "",
                "## Explicit deliverables",
                "",
                "| Deliverable | Coverage |",
                "|---|---|",
                "| Architecture document | DOC1 |",
                "| Risk register | DOC2 |",
                "```",
                "",
                "Rules for the Coverage column:",
                "- Task id (e.g., `WEB2`) → that task addresses the requirement",
                "- `defer: <one-line reason>` → out of scope for current phase",
                "- `?` / `TBD` / empty → integrity check will flag it on the",
                "  next turn. Don't leave these in the final output.",
                "",
                "### Other rules for this bootstrap turn",
                "",
                "Mark assumptions with `> assumption: …` inside file bodies.",
                "Every task body ends with `## Done when` (2-5 acceptance",
                "criteria, observable, present tense).",
                "",
                "When done writing, reply with: (a) one short paragraph summary,",
                "(b) at MOST two open questions whose answers would materially",
                "change the plan. Do NOT paste file contents back — the",
                "cockpit auto-refreshes within ~2 seconds.",
                "",
                "If the user said almost nothing (literally 'hi', 'test',",
                "one-word), ask ONE focused question and stop. Never more.",
                "",
                "Once this turn lands files, the cluster is no longer empty",
                "and this section disappears from future briefings.",
            ]
        )

    def _estimate_brief_size(self) -> int:
        """Best-available signal for how big the project brief is.
        Drives the bootstrap task-count target. Sources, in order:
        (1) context.md if present, (2) accumulated chat.user texts in
        the current conv from .meshkore/timeline/, (3) the current
        user_text. Returns total chars."""
        # Source 1: context.md (already written on prior turns).
        ctx = self.paths.docs_dir / "context.md"
        if ctx.exists():
            try:
                size = len(ctx.read_text(errors="replace"))
                if size > 0:
                    return size
            except OSError:
                pass
        # Source 2: sum of all chat.user texts in this conv.
        total = 0
        try:
            if self.paths.timeline_dir.exists():
                for f in sorted(self.paths.timeline_dir.glob("*.jsonl")):
                    try:
                        for line in f.read_text(errors="replace").splitlines():
                            try:
                                ev = json.loads(line)
                            except json.JSONDecodeError:
                                continue
                            if (
                                ev.get("conv") == self.conv
                                and ev.get("type") == "chat.user"
                            ):
                                total += len(ev.get("text") or "")
                    except OSError:
                        continue
        except Exception:
            pass
        if total > 0:
            return total
        # Source 3: this turn's user_text.
        return len(self.user_text or "")

    def _section_integrity(self) -> str:
        violations = self.integrity.check()
        if not violations:
            return ""
        lines = [
            "## Integrity hints (please fix as part of this turn)",
            "",
            f"State-integrity check found {len(violations)} issue(s) you",
            "can resolve quickly. They are NOT blocking — proceed with the",
            "user's request first, then fix as you go.",
            "",
        ]
        for v in violations:
            kind = v.get("kind", "unknown")
            fix = v.get("fix", "(no fix suggested)")
            if kind == "module_not_declared":
                lines.append(f"- **Orphan module** `{v.get('module_id')}` — {fix}")
            elif kind == "task_initiative_broken":
                lines.append(
                    f"- **Broken initiative ref** task=`{v.get('task_id')}`"
                    f" → initiative=`{v.get('initiative_ref')}` — {fix}"
                )
            elif kind == "initiative_without_tasks":
                lines.append(
                    f"- **Initiative without tasks** `{v.get('initiative_id')}`"
                    f" (status: `{v.get('status')}`) — {fix}"
                )
            elif kind == "initiative_too_dense":
                lines.append(
                    f"- **Initiative too dense** `{v.get('initiative_id')}`"
                    f" carries {v.get('child_count')} active/next tasks"
                    f" — {fix}"
                )
            elif kind == "context_coverage_gap":
                toks = v.get("tokens") or []
                pretty = ", ".join(
                    f"`{t.get('token')}` ({t.get('mentions')}×)" for t in toks
                )
                lines.append(
                    f"- **Potential coverage gaps (tokens)** — {pretty} — {fix}"
                )
            elif kind == "coverage_doc_missing":
                lines.append(f"- **Coverage matrix missing** — {fix}")
            elif kind == "coverage_gaps_in_doc":
                n = v.get("count", "?")
                lines.append(f"- **Coverage matrix has {n} unresolved row(s)** — {fix}")
            else:
                lines.append(f"- **{kind}** — {fix}")
        return "\n".join(lines)

    def _section_cockpit_context(self) -> str:
        if not self.context_docs:
            return ""
        lines = [
            "## Context attached by the operator's cockpit",
            "",
            "The architect cockpit sent these documents alongside the",
            "user's message. Treat them as authoritative context for this",
            "turn (operator's intent, scope, recent UI state).",
            "",
        ]
        for doc in self.context_docs:
            if not isinstance(doc, dict):
                continue
            fname = doc.get("filename") or "(unnamed)"
            content = (doc.get("content") or "").strip()
            if not content:
                continue
            lines.append(f"### `{fname}`")
            lines.append("")
            lines.append(content)
            lines.append("")
        return "\n".join(lines).rstrip()

    def _section_history(self) -> str:
        turns = _conversation_history(self.paths, self.conv)
        if not turns:
            return ""
        return "## Recent turns in this conversation\n\n" + "\n".join(turns)

    def _section_consult_addendum(self) -> str:
        """py-1.10.8 — When the roadmap-architect dispatches a question
        to the onboarding/_onboarding_v1 conv with the [architect-consult]
        prefix, A001 (the project coordinator) must DECIDE on the
        operator's behalf, not bounce back. This addendum is injected
        only for that exact pattern."""
        body = (self.user_text or "").strip()
        if not body.startswith("[architect-consult]"):
            return ""
        if self.conv != "_onboarding_v1":
            return ""
        return (
            "## [architect-consult] mode — DECIDE, don't bounce\n\n"
            "The roadmap-architect is mid-pass and needs a decision YOU "
            "must make on the user's behalf. The user pressed Run all "
            "specifically so they would NOT be in the loop. Bouncing "
            "the question to them defeats the whole feature.\n\n"
            "Your authority for this turn:\n"
            "- You have full power to pick. The architect will execute "
            "whatever you say.\n"
            "- Read `.meshkore/agents/_types/custom/memory.md`, "
            "`.meshkore/roadmap/initiatives/*.md`, recent chat history, "
            "any README, the project vision — anything that surfaces "
            "the user's preferences. Pick the option most aligned.\n"
            "- When in doubt, prefer: the simpler option, the cheaper "
            "option, the option that keeps shipping velocity, the "
            "option that matches the cluster's existing tech defaults.\n\n"
            "Reply format — STRICT:\n"
            "- ONE paragraph, <80 words.\n"
            "- First sentence: the decision in plain language.\n"
            "- Second sentence: one-line rationale.\n"
            "- That's it. No preamble, no caveats, no \"happy to "
            'discuss", no follow-up question.\n\n'
            "If — and ONLY if — the question is genuinely about the "
            "PRODUCT IDEA (not implementation, not tech choice, not "
            "design defaults), reply with the literal string:\n"
            "    DEFER:<one-line reason what's conceptually unclear>\n"
            "The architect will defer that single task to the end of "
            "the pass and continue with the rest. Use this sparingly — "
            "it's the only escape valve and it shouldn't be your "
            "default."
        )

    def _section_user_turn(self) -> str:
        body = self.user_text.strip() if self.user_text else ""
        if not body:
            return "## User just said\n\n(empty message)"
        return "## User just said\n\n" + body
