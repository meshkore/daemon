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

# Largest request body the HTTP surface will read. DAH1 — this used to be
# declared TWICE (routes.py + daemon.py) with the same value; the wire layer
# now imports the one in this leaf, so there is a single number to change.
MAX_BODY_BYTES = 4 * 1024 * 1024  # 4 MB — protect against runaway POSTs

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
# DAH1 — the version literal is REGEX-PARSED by three separate code paths
# (bundle.py's 8 KB version marker, selfupdate._fetch_remote_version's
# Range-request over the first 8 KB of the published bundle, and
# bootupdate's `^DAEMON_VERSION\s*=\s*"..."` match on the download). It used
# to carry a ~1,600-character release note as a trailing comment, which is
# ~20% of the 8 KB window those two Range readers get — one more paragraph
# and a remote version check would silently start returning None. Release
# notes belong in CHANGELOG.md; this line stays one short line forever.
# py-1.35.1 — "short" is also a FORMATTER constraint, not just a budget:
# past the line limit `ruff format` rewrites this as `DAEMON_VERSION = (\n
#     "py-X.Y.Z"  # …\n)`, and all three regexes are anchored to the
# single-line form. bundle.py refuses to build (loud, good) — but keep the
# comment short enough that the formatter never wants to wrap it.
DAEMON_VERSION = "py-1.35.1"  # DAH7 — cancelled tasks unblock archival; CHANGELOG.md
