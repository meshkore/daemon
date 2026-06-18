"""uploads.py — UploadStore — per-conv image/file upload store.

Extracted from storage.py (daemon-architecture-v2 Phase 3d). Verbatim move;
imported back where used."""

from __future__ import annotations

import base64
import secrets
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional

from paths import Paths
from utils import _iso_now, _log


class UploadStore:
    """py-1.12.21 — chat attachment persistence.

    Stores image / binary attachments sent with /chat/dispatch under
    `.meshkore/uploads/<YYYY-MM-DD>/<filename>`. Returns a small
    manifest record that the daemon embeds in the matching chat.user
    timeline event, so the cockpit can render thumbnails on next
    hydrate. Retention is bounded by
    `cluster.yaml.uploads.retention_days` (default 30); a sweep runs
    opportunistically on every save.

    File-name shape:
        `<conv-slug>-<ms-ts>-<idx>-<rand4>.<ext>`
    Lexicographic ordering matches chronology + idx, and the random
    suffix avoids collisions when two uploads land in the same ms.
    """

    DEFAULT_RETENTION_DAYS = 30
    MAX_BYTES_PER_FILE = 8 * 1024 * 1024  # 8 MB, claude-code's friendly upper bound
    MAX_FILES_PER_DISPATCH = 12

    # Media-type → file extension. Anything outside this map gets
    # `.bin`, which the cockpit can still link / download but won't
    # render as <img>.
    _EXT_BY_MEDIA: Dict[str, str] = {
        "image/png": "png",
        "image/jpeg": "jpg",
        "image/jpg": "jpg",
        "image/gif": "gif",
        "image/webp": "webp",
        "image/svg+xml": "svg",
        "image/avif": "avif",
        "image/bmp": "bmp",
    }

    def __init__(self, paths: Paths, cluster: Any) -> None:
        self.paths = paths
        self.cluster = cluster

    def _retention_days(self) -> int:
        try:
            data = self.cluster.data if isinstance(self.cluster.data, dict) else {}
            cfg = data.get("uploads") if isinstance(data.get("uploads"), dict) else None
            if cfg is None:
                return self.DEFAULT_RETENTION_DAYS
            n = int(cfg.get("retention_days", self.DEFAULT_RETENTION_DAYS))
            return max(0, min(365, n))
        except Exception:
            return self.DEFAULT_RETENTION_DAYS

    def _safe_slug(self, s: str) -> str:
        out = []
        for c in s:
            if c.isalnum() or c in "-_":
                out.append(c)
            else:
                out.append("_")
        return ("".join(out) or "x")[:48]

    def save_dispatch(
        self,
        *,
        conv: str,
        images: Optional[List[Dict[str, Any]]],
        ts_iso: str,
        skipped: Optional[List[Dict[str, Any]]] = None,
    ) -> List[Dict[str, Any]]:
        """Persist the dispatch's images list. Returns a manifest list
        ready to be embedded in the chat.user event. Each entry:

            {
              "kind": "image",
              "media_type": "image/png",
              "url": "/chat/uploads/2026-06-10/<file>",
              "size_bytes": 12345,
              "filename": "<file>"
            }

        Silently skips entries that fail validation; never raises."""
        if not images:
            return []
        try:
            self._gc_old()
        except Exception as e:
            _log(f"upload gc failed: {e}")
        out: List[Dict[str, Any]] = []
        # Daily bucket — yyyy-mm-dd, gitignored.
        bucket = ts_iso[:10] if len(ts_iso) >= 10 else _iso_now()[:10]
        bucket_dir = self.paths.uploads_dir / bucket
        bucket_dir.mkdir(parents=True, exist_ok=True)
        # Single millisecond timestamp shared across this dispatch so
        # the operator's batch sorts together in `ls -lt`.
        ms = int(time.time() * 1000)
        conv_slug = self._safe_slug(conv)
        for idx, img in enumerate(images[: self.MAX_FILES_PER_DISPATCH]):
            if not isinstance(img, dict):
                continue
            media_type = str(img.get("media_type") or "image/png").lower()
            data_b64 = img.get("data")
            if not isinstance(data_b64, str) or not data_b64:
                continue
            try:
                blob = base64.b64decode(data_b64, validate=True)
            except Exception:
                # D-UPLOAD-FEEDBACK-01 — surface the drop instead of silence.
                if skipped is not None:
                    skipped.append(
                        {
                            "idx": idx,
                            "reason": "decode_failed",
                            "media_type": media_type,
                        }
                    )
                continue
            if len(blob) == 0:
                if skipped is not None:
                    skipped.append(
                        {"idx": idx, "reason": "empty", "media_type": media_type}
                    )
                continue
            if len(blob) > self.MAX_BYTES_PER_FILE:
                if skipped is not None:
                    skipped.append(
                        {
                            "idx": idx,
                            "reason": "too_large",
                            "media_type": media_type,
                            "size_bytes": len(blob),
                            "max_bytes": self.MAX_BYTES_PER_FILE,
                        }
                    )
                continue
            ext = self._EXT_BY_MEDIA.get(media_type, "bin")
            rand4 = secrets.token_hex(2)
            fname = f"{conv_slug}-{ms}-{idx}-{rand4}.{ext}"
            path = bucket_dir / fname
            try:
                path.write_bytes(blob)
            except OSError as e:
                _log(f"upload save failed for {fname}: {e}")
                continue
            out.append(
                {
                    "kind": "image",
                    "media_type": media_type,
                    "url": f"/chat/uploads/{bucket}/{fname}",
                    "size_bytes": len(blob),
                    "filename": fname,
                }
            )
        return out

    def serve_path(self, bucket: str, filename: str) -> Optional[Path]:
        """Resolve `<uploads>/<bucket>/<filename>` if it's safe to
        serve. Returns None on traversal attempts or missing file."""
        if (
            not bucket
            or not filename
            or ".." in bucket
            or ".." in filename
            or "/" in filename
            or "\\" in filename
            or "/" in bucket
            or "\\" in bucket
        ):
            return None
        # bucket should be YYYY-MM-DD shaped.
        if len(bucket) != 10 or bucket[4] != "-" or bucket[7] != "-":
            return None
        path = (self.paths.uploads_dir / bucket / filename).resolve()
        try:
            path.relative_to(self.paths.uploads_dir.resolve())
        except ValueError:
            return None
        if not path.is_file():
            return None
        return path

    def _gc_old(self) -> None:
        days = self._retention_days()
        if days <= 0:
            return
        root = self.paths.uploads_dir
        if not root.is_dir():
            return
        cutoff = (datetime.now(timezone.utc) - timedelta(days=days)).strftime(
            "%Y-%m-%d",
        )
        for entry in root.iterdir():
            if not entry.is_dir():
                continue
            name = entry.name
            # Only sweep YYYY-MM-DD-shaped buckets.
            if len(name) != 10 or name[4] != "-" or name[7] != "-":
                continue
            if name < cutoff:
                try:
                    import shutil as _shutil

                    _shutil.rmtree(entry, ignore_errors=True)
                except Exception as e:
                    _log(f"upload gc rmtree({entry}) failed: {e}")
