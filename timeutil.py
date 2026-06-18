"""timeutil.py — UTC ISO-8601 timestamp helpers.

Extracted from utils.py (daemon-architecture-v2 Phase 3d). Verbatim move;
imported back where used."""

from __future__ import annotations

from datetime import datetime, timezone


def _iso_now() -> str:
    return (
        datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.")
        + f"{datetime.now(timezone.utc).microsecond // 1000:03d}Z"
    )


def _iso_at(epoch_secs: int) -> str:
    """ISO-8601 UTC for a given epoch — used for pause expiry stamps
    (py-1.10.26). Cheap; no ms component (we only care about the
    minute granularity for rate-limit cooldowns)."""
    return datetime.fromtimestamp(epoch_secs, timezone.utc).strftime(
        "%Y-%m-%dT%H:%M:%SZ"
    )
