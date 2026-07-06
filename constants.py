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
DAEMON_VERSION = "py-1.30.2"  # 1.30.2 — TEAM-EXT AUTO-ARCHIVE (initiative `team-external-gateway` follow-up). External asks to a PROFILE member (consultant, etc.) were never archived once their turn finished, so `ext-<member>-<session|stamp>` convs piled up forever as non-archived rows and cluttered the operator's chat-rail agents column (they are one-shot/session API artifacts, not operator-facing sessions). `teamext.py` `_teamext_watch` now takes an `auto_archive` flag (true for `kind: profile`, false for singletons — architect-master/roadmap-orchestrator keep their ONE visible working conversation by design) and archives the conv in the watcher's `finally` block, covering all three terminal paths (done, idle-timeout error, watch-deadline error) plus any unexpected exception. Archiving is cosmetic only — `chat_dispatch` never checks the archived flag, so a later ask on the SAME `session` still works exactly as before (it just re-archives on completion); History → Archived still shows every one. No Standard bump (daemon-local capability, mirrors how TEG-2/CPL-1 shipped).
