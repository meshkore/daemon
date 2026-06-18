"""runrotator.py — TimelineRotator — timeline JSONL rotation.

Extracted from runs.py (daemon-architecture-v2 Phase 3d). Verbatim move;
imported back where used."""

from __future__ import annotations

import threading
import time

from paths import Paths
from runs import TIMELINE_ROTATE_AGE_DAYS, TIMELINE_ROTATE_SCAN_SEC
from utils import _log


class TimelineRotator:
    """Background gzipper for old jsonl files in .meshkore/timeline/."""

    def __init__(
        self,
        paths: "Paths",
        age_days: int = TIMELINE_ROTATE_AGE_DAYS,
        delete_days: int = 0,
    ):
        self.paths = paths
        self.age_days = age_days
        # py-1.16.1 (D-STORE-RETENTION-01) — delete archived .gz this many
        # days AFTER rotation. 0 = never delete (opt-in): auto-pruning chat
        # history requires an explicit cluster.yaml `storage.retention_days`.
        # Effective history ≈ age_days + delete_days.
        self.delete_days = delete_days
        self._stop = threading.Event()
        threading.Thread(target=self._loop, daemon=True).start()

    def shutdown(self) -> None:
        self._stop.set()

    def _loop(self) -> None:
        # Run once at boot (after a brief delay so we don't fight with
        # the cluster's first state.json rebuild), then every hour.
        if self._stop.wait(60.0):
            return
        while True:
            try:
                self.rotate_once()
            except Exception as e:
                _log(f"timeline rotator: {e}")
            if self._stop.wait(TIMELINE_ROTATE_SCAN_SEC):
                return

    def rotate_once(self) -> int:
        if not self.paths.timeline_dir.exists():
            return 0
        cutoff = time.time() - (self.age_days * 86400)
        archive_dir = self.paths.timeline_dir / "archive"
        rotated = 0
        for f in self.paths.timeline_dir.glob("*.jsonl"):
            try:
                st = f.stat()
            except OSError:
                continue
            if st.st_mtime > cutoff:
                continue  # too recent
            # Compress in place, move the .gz to archive/, delete the
            # original. Keep one log line per rotation so the operator
            # can audit it from the daemon's stderr.
            try:
                archive_dir.mkdir(parents=True, exist_ok=True)
                import gzip

                gz_path = archive_dir / (f.name + ".gz")
                if gz_path.exists():
                    # Already rotated — just delete the source.
                    f.unlink()
                    rotated += 1
                    continue
                with open(f, "rb") as src, gzip.open(gz_path, "wb") as dst:
                    while True:
                        chunk = src.read(64 * 1024)
                        if not chunk:
                            break
                        dst.write(chunk)
                f.unlink()
                _log(f"timeline rotator: {f.name} → archive/{gz_path.name}")
                rotated += 1
            except OSError as e:
                _log(f"timeline rotator: skipped {f.name}: {e}")
        if self.delete_days > 0:
            self._delete_old_archives()
        return rotated

    def _delete_old_archives(self) -> int:
        """py-1.16.1 (D-STORE-RETENTION-01) — opt-in prune of archived .gz
        older than `delete_days` (by file mtime = rotation time). Off when
        delete_days==0. Bounds otherwise-unbounded timeline growth."""
        archive_dir = self.paths.timeline_dir / "archive"
        if not archive_dir.exists():
            return 0
        cutoff = time.time() - (self.delete_days * 86400)
        deleted = 0
        for gz in archive_dir.glob("*.gz"):
            try:
                if gz.stat().st_mtime < cutoff:
                    gz.unlink()
                    deleted += 1
            except OSError:
                pass
        if deleted:
            _log(
                f"timeline retention: deleted {deleted} archived .gz "
                f"older than {self.delete_days}d"
            )
        return deleted
