"""integrity.py — project-state inspection for the briefing pipeline.

Extracted from prompts.py (daemon-architecture-v2 Phase 3b). ProjectState
(cheap lazy FS summary) + StateIntegrityChecker (orphan-module / broken-ref
detection) + the _COVERAGE_STOPWORDS set move VERBATIM. Used only by
BriefingPipeline, which imports them back."""

from __future__ import annotations

from typing import Any, List, Optional

from paths import Paths


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
