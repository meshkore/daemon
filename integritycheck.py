"""integritycheck.py — StateIntegrityChecker — orphan/broken-ref detection.

Extracted from integrity.py (daemon-architecture-v2 Phase 3d). Verbatim move;
imported back where used."""

from __future__ import annotations

import re
from typing import Any, Dict, List, Optional, Tuple

from integrity import ProjectState
from paths import Paths
from utils import parse_frontmatter


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
