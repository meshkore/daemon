"""fsread.py — extracted from readapi.py (daemon-architecture-v2 Phase 3d).

FsReadMixin: methods moved VERBATIM out of QueryMixin; Daemon inherits both so
every self.* resolves on the combined instance -> byte-identical."""

from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional

from registries import _split_frontmatter
from utils import _iso_now, parse_frontmatter


class FsReadMixin:
    def initiative_activity(self, initiative_id: str) -> Dict[str, Any]:
        """py-1.9.3 — Walk git log for commits referencing this initiative.
        Returns at most 50 of the most recent matching commits, each with
        the files it touched (`git diff-tree --no-commit-id --name-only -r`).
        Matching is plain substring on subject + body so operators can
        reference an initiative however they like ("[I-cron-dashboard]",
        "for cron-dashboard", etc.) — no rigid trailer schema.

        Bounded by 1000 commits scanned + a hard timeout per git call so
        a 50k-commit repo doesn't melt the daemon. Failures (no git, bad
        repo, timeout) degrade to an empty payload + an explanatory
        `error` field; the cockpit just shows "no activity yet".
        """
        out: Dict[str, Any] = {
            "initiative_id": initiative_id,
            "commits": [],
            "generated_at": _iso_now(),
        }
        if not isinstance(initiative_id, str) or not initiative_id.strip():
            out["error"] = "invalid initiative id"
            return out
        iid = initiative_id.strip()

        import subprocess as _sp

        root = self.paths.root

        # py-1.9.3 — Multi-repo workspaces (meshkore-style: webapp/,
        # architect/, .meshkore/ each a separate git repo at depth 1)
        # AND single-repo projects (typical ikamiro-style) both work.
        # Find every depth ≤ 1 directory that owns a `.git` and scan
        # each one. The commit row carries a `repo` field so the
        # cockpit can disambiguate when two repos both reference the
        # same initiative id.
        repo_dirs: List[Path] = []
        if (root / ".git").exists():
            repo_dirs.append(root)
        else:
            try:
                for child in sorted(root.iterdir()):
                    if not child.is_dir() or child.name.startswith("."):
                        continue
                    if (child / ".git").exists():
                        repo_dirs.append(child)
            except OSError:
                pass

        if not repo_dirs:
            out["error"] = "no git repos found at project root or depth-1"
            return out

        def git_in(cwd: Path, *args: str, timeout: float = 4.0) -> Optional[str]:
            try:
                r = _sp.run(
                    ["git", "-C", str(cwd), *args],
                    capture_output=True,
                    text=True,
                    timeout=timeout,
                )
                if r.returncode != 0:
                    return None
                return r.stdout
            except (_sp.TimeoutExpired, FileNotFoundError, OSError):
                return None

        commits: List[Dict[str, Any]] = []
        for repo_dir in repo_dirs:
            repo_label = repo_dir.name if repo_dir != root else "(root)"
            raw = git_in(
                repo_dir,
                "log",
                "--max-count=1000",
                "--grep",
                iid,
                "-i",
                "--pretty=format:%H%x09%h%x09%aI%x09%an%x09%s",
                timeout=6.0,
            )
            if raw is None:
                continue
            for line in raw.splitlines():
                if not line.strip():
                    continue
                parts = line.split("\t", 4)
                if len(parts) != 5:
                    continue
                sha, short, ts, author, subject = parts
                files_raw = (
                    git_in(
                        repo_dir,
                        "diff-tree",
                        "--no-commit-id",
                        "--name-only",
                        "-r",
                        sha,
                        timeout=3.0,
                    )
                    or ""
                )
                files = [ln.strip() for ln in files_raw.splitlines() if ln.strip()]
                commits.append(
                    {
                        "repo": repo_label,
                        "sha": sha,
                        "short_sha": short,
                        "ts": ts,
                        "author": author,
                        "subject": subject,
                        "files": files[:200],
                        "files_truncated": len(files) > 200,
                    }
                )
                if len(commits) >= 50:
                    break
            if len(commits) >= 50:
                break

        # Newest first across repos (each repo's slice already comes
        # newest-first from git log, but interleaved across repos
        # needs an explicit ts sort).
        commits.sort(key=lambda c: c.get("ts") or "", reverse=True)
        out["commits"] = commits[:50]
        return out

    def context_tree(self) -> Dict[str, Any]:
        """py-1.14.1 — Standard v14 §3.5 project context tree.

        Walks `.meshkore/context/` and returns the nested folder/file
        shape the cockpit's Context tab renders: per-file `title`
        (frontmatter `title`, falling back to a humanized filename),
        `updated` + `status` (frontmatter), word count, and an
        `over_cap` flag against the §3.5 brevity caps. Tree-level the
        response carries `total_words`, `token_estimate` (~1.5 tokens /
        word), the 4500-token budget, an `over_budget` flag, and a
        `warnings` list (per-file over-cap notes + total-over-budget).

        File bodies are NOT inlined — the cockpit lazy-fetches each on
        selection via `/context/<path>`. Returns `exists: False` with
        an empty tree when no `.meshkore/context/` directory is present
        (e.g. a freshly bootstrapped cluster) so the cockpit can render
        its empty-state hint instead of an error.

        Path traversal is structurally impossible here — we only ever
        `iterdir()` inside `context_dir`; `path` values are relative to
        that root and consumed by `/context/<path>` which re-validates.
        """
        root = self.paths.context_dir
        warnings: List[str] = []

        def humanize(name: str) -> str:
            stem = name[:-3] if name.endswith(".md") else name
            return stem.replace("-", " ").replace("_", " ").strip().capitalize()

        def word_count(text: str) -> int:
            # Count words in the body only (frontmatter excluded) so the
            # cap reflects prose, not YAML keys.
            _fm, body = _split_frontmatter(text)
            return len(body.split())

        def build_file(fp: "Path", rel: str, cap: Optional[int]):
            title = humanize(fp.name)
            updated: Optional[str] = None
            status: Optional[str] = None
            words = 0
            try:
                text = fp.read_text(encoding="utf-8", errors="replace")
                fm = parse_frontmatter(text)
                if isinstance(fm.get("title"), str) and fm["title"].strip():
                    title = fm["title"].strip()
                if isinstance(fm.get("updated"), str):
                    updated = fm["updated"].strip()
                elif fm.get("updated") is not None:
                    updated = str(fm["updated"])
                if isinstance(fm.get("status"), str) and fm["status"].strip():
                    status = fm["status"].strip()
                words = word_count(text)
            except OSError:
                pass
            over_cap = cap is not None and words > cap
            if over_cap:
                warnings.append(f"{rel}: {words}w over the {cap}w cap")
            node: Dict[str, Any] = {
                "kind": "file",
                "name": fp.name,
                "path": rel,
                "title": title,
                "words": words,
                "over_cap": over_cap,
            }
            if updated:
                node["updated"] = updated
            if status:
                node["status"] = status
            return node, words

        def cap_for(rel: str, name: str, in_folder: bool) -> Optional[int]:
            if in_folder:
                # README.md is an index, exempt; other entries cap at 100.
                return None if name == "README.md" else self.CONTEXT_FOLDER_ENTRY_CAP
            return self.CONTEXT_WORD_CAPS.get(name)

        total_words = 0

        def build_dir(dp: "Path", rel_prefix: str, in_folder: bool):
            nonlocal total_words
            children: List[Dict[str, Any]] = []
            try:
                entries = sorted(dp.iterdir(), key=lambda e: e.name)
            except OSError:
                return children
            # Files first (alpha), then sub-dirs — but keep README.md at
            # the top of a folder so the cockpit's "click dir → README"
            # affordance lands on the index.
            files = [
                e
                for e in entries
                if e.is_file()
                and e.suffix.lower() == ".md"
                and not e.name.startswith(".")
            ]
            dirs = [e for e in entries if e.is_dir() and not e.name.startswith(".")]
            files.sort(key=lambda e: (e.name != "README.md", e.name))
            for f in files:
                rel = f"{rel_prefix}{f.name}"
                node, words = build_file(f, rel, cap_for(rel, f.name, in_folder))
                total_words += words
                children.append(node)
            for d in dirs:
                rel = f"{rel_prefix}{d.name}"
                sub = build_dir(d, f"{rel}/", in_folder=True)
                children.append(
                    {
                        "kind": "dir",
                        "name": d.name,
                        "path": rel,
                        "title": humanize(d.name),
                        "children": sub,
                    }
                )
            return children

        if not root.is_dir():
            return {
                "exists": False,
                "root": ".meshkore/context",
                "total_words": 0,
                "token_estimate": 0,
                "budget_tokens": self.CONTEXT_BUDGET_TOKENS,
                "over_budget": False,
                "warnings": [],
                "tree": [],
            }

        tree = build_dir(root, "", in_folder=False)
        token_estimate = int(round(total_words * 1.5))
        over_budget = token_estimate > self.CONTEXT_BUDGET_TOKENS
        if over_budget:
            warnings.append(
                f"context is {token_estimate} tokens — over the "
                f"{self.CONTEXT_BUDGET_TOKENS}-token budget (§3.5)"
            )
        return {
            "exists": True,
            "root": ".meshkore/context",
            "total_words": total_words,
            "token_estimate": token_estimate,
            "budget_tokens": self.CONTEXT_BUDGET_TOKENS,
            "over_budget": over_budget,
            "warnings": warnings,
            "tree": tree,
        }

    def log_listing(self) -> List[Dict[str, Any]]:
        """py-1.9.0 — Descending-by-date list of `.meshkore/log/*.md`
        narrative day-files. Just metadata (name, date, size, mtime);
        callers fetch the body via `/log/<filename>` for paged display
        in the cockpit Diary tab. Dotfiles + non-.md files are skipped.

        Returned shape:
            [{ "name": "2026-05-27.md", "date": "2026-05-27",
               "size": 12345, "mtime": "2026-05-27T21:00:00Z" }]
        """
        if not self.paths.log_dir.exists():
            return []
        out = []
        for f in self.paths.log_dir.iterdir():
            if not f.is_file() or f.name.startswith("."):
                continue
            if f.suffix.lower() != ".md":
                continue
            # Most filenames are `YYYY-MM-DD.md`. The few that aren't
            # (handoff notes etc.) get `date: null`.
            stem = f.stem
            date = (
                stem
                if (
                    len(stem) == 10
                    and stem[4] == "-"
                    and stem[7] == "-"
                    and stem[:4].isdigit()
                    and stem[5:7].isdigit()
                    and stem[8:10].isdigit()
                )
                else None
            )
            try:
                st = f.stat()
                size = st.st_size
                mtime = datetime.fromtimestamp(st.st_mtime, tz=timezone.utc).strftime(
                    "%Y-%m-%dT%H:%M:%SZ"
                )
            except OSError:
                size = None
                mtime = None
            out.append(
                {
                    "name": f.name,
                    "date": date,
                    "size": size,
                    "mtime": mtime,
                }
            )
        # Dated entries descending (newest → oldest), then any extras
        # (handoff notes etc.) appended in stable filename order.
        dated = sorted(
            [e for e in out if e["date"]], key=lambda e: e["date"], reverse=True
        )
        extras = sorted([e for e in out if not e["date"]], key=lambda e: e["name"])
        return dated + extras
