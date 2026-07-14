"""constants.py — leaf module: the daemon's load-bearing constants.

Extracted (DA-CONST-01, daemon-architecture-v2) so version/port config
lives in a dependency-free leaf that ANY module can import without a
cycle back to daemon.py — unblocking the selfupdate/bootstrap splits.
No sibling imports. bundle.py reads DAEMON_VERSION from HERE for the
early 8 KB version marker; MODULES inlines constants.py FIRST.
"""

from __future__ import annotations

import os
from pathlib import Path


PORT_RANGE = (5570, 5589)

# Release-signing PUBLIC key (Ed25519, hex). py-1.27.5. The daemon verifies
# every self-update bundle's detached signature (`<url>.sig`) against this
# pinned key before swapping + re-exec'ing — so a CDN compromise / MITM
# can't push code that runs as the operator. The matching PRIVATE seed
# lives ONLY at daemon/.release-signing-key (gitignored, off-CDN) and is
# used by bundle.py at release time. Rotating the key = regenerate the
# seed, re-pin this hex, redeploy (old daemons that already trust the old
# key will reject the new build until they update through a key-overlap
# release — see workflows/W2). Empty string = signature checks disabled.
RELEASE_PUBKEY_HEX = "9699b5c93066195d85e974a1bca9ace6931ea31a21e347414d6f0a34d55b13cb"
# py-1.15.0 — machine-global sticky port registry (cluster_id → port).
# Lives outside any repo so every daemon on this box shares one source of
# truth and a cluster ALWAYS comes back up on the same port (no drift).
# py-1.16.0 (D-TEST-ISO-01) — MESHKORE_PORTS_FILE overrides the registry
# path so the test suite (which spawns real daemon subprocesses) points it
# at a tmp file instead of polluting the operator's real ~/.meshkore.
_PORT_REGISTRY_FILE = Path(
    os.environ.get("MESHKORE_PORTS_FILE") or (Path.home() / ".meshkore" / "ports.json")
)
_PORT_REGISTRY_DIR = _PORT_REGISTRY_FILE.parent
FS_POLL_SEC = 1.5
DAEMON_VERSION = "py-1.32.2"  # 1.32.2 — CONV-META MEMBER SURFACE FIX (operator field report 2026-07-13: switching architect-master to Z.AI/glm-4.6 had no effect on its normal chat turns). Root cause: `chat_snapshot`/`chat_convs` (chatread.py) never surfaced the per-conv `member` binding, so the cockpit could never learn/heal it for a conv that predates the field (notably `_onboarding_v1`, the master's own long-lived system conv, created 2026-05-30) — every dispatch from the normal chat UI on such a conv silently omitted `member`, so `_member_dispatch_prep` never ran and the member's client/model/provider dial (verified working end-to-end via the external ask/poll gateway, which always resolves the member fresh) had zero effect. FIX: `chat_convs()` now includes `member`/`provider` in its per-conv entry; the cockpit's `hydrateFromSnapshot` seeds BOTH on a fresh conv and — critically — HEALS `member` into any pre-existing local convMeta entry that's missing it, exactly mirroring the existing `agentId`-healing pattern. No schema/wire-format change beyond two additive fields.
