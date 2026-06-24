#!/usr/bin/env python3
"""Bundle the daemon package into a single self-contained ``dist/daemon.py``.

DM3+ form: walks the source modules in dependency order, strips the
sibling-module imports (each module's body becomes part of one big
file's global namespace, so the cross-module names resolve naturally),
prepends an auto-generated header, writes ``dist/daemon.py``.

Dependency order (top of bundle → bottom):

    paths.py    — Paths + TLS constants. No sibling deps.
    storage.py  — ChatArchive, StorageReport, UploadStore,
                  ChatQueueManager. Depends on paths.
    daemon.py   — wiring + main + everything else. Depends on both.

Shadowing rule: when paths.py / storage.py and daemon.py both define
the same name (notably ``_log`` / ``_iso_now`` — storage.py keeps
local copies for source-tree dev; daemon.py has the full
debug-stream-aware versions), the LATER definition wins in Python
module namespace. Bundle ordering must keep daemon.py last so
production gets the canonical helpers."""

from __future__ import annotations

import re
import subprocess
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent
SRC = ROOT / "daemon.py"
DIST = ROOT / "dist"
OUT = DIST / "daemon.py"

# Sibling modules to inline ahead of daemon.py, in dep order. An entry may
# be a FILE (``foo.py``) or a PACKAGE FOLDER (``agent_prompts`` — no .py).
# A folder is expanded (DA-BUNDLER-01) into its ``*.py`` in deterministic
# order: every non-dunder file sorted, then ``__init__.py`` LAST so it can
# assemble names the fragment files defined above it. This lets a large
# module be split into a ≤300-LOC granular subfolder while the single-file
# bundle stays one flat namespace.
MODULES = [
    "constants.py",
    "paths.py",
    "crypto_ed25519.py",
    "fsatomic.py",
    "timeutil.py",
    "yamlparse.py",
    "timeline.py",
    "utils.py",
    "debuglog.py",
    "cluster.py",
    "scaffold.py",
    "hub.py",
    "registries.py",
    "workflows.py",
    "statebuild.py",
    "state.py",
    "render.py",
    "verify.py",
    "runs.py",
    "runrotator.py",
    "storage.py",
    "uploads.py",
    "chatqueue.py",
    "chat.py",
    "chatreaper.py",
    "http_server.py",
    "bootstrap.py",
    "bootupdate.py",
    "selfupdate.py",
    "anchor.py",
    "anchorprogress.py",
    "cron.py",
    "cronsched.py",
    "agent_prompts",  # package folder — see _expand_module()
    "agent_types.py",
    "integrity.py",
    "integritycheck.py",
    "prompts.py",
    "coordination.py",
    "coordwake.py",
    "pausemgr.py",
    "readapi.py",
    "fsread.py",
    "walls.py",
    "chatread.py",
    "credapi.py",
    "runnerutil.py",
    "runneranchor.py",
    "runnerloop.py",
    "runnerspawn.py",
    "runner.py",
    "chatsvc.py",
    "convmeta.py",
    "chatspawn.py",
    "crud.py",
    "quota.py",
    "quotaprober.py",
    "routes_get.py",
    "routes_post.py",
    "routes.py",
    "selfupdatesvc.py",
    "verifysvc.py",
    "lifecycle.py",
    "projectctx.py",  # DC-1 — per-project state; after all its stores, before daemon.py
]


def _expand_module(entry: str) -> list[Path]:
    """Resolve a MODULES entry to the ordered list of source files to
    inline. A ``.py`` file → itself. A package folder → its fragment
    ``*.py`` (sorted) followed by ``__init__.py`` last."""
    p = ROOT / entry
    if entry.endswith(".py"):
        return [p]
    if p.is_dir():
        files = sorted(f for f in p.glob("*.py") if f.name != "__init__.py")
        init = p / "__init__.py"
        if init.exists():
            files.append(init)
        return files
    raise SystemExit(
        f"bundle.py: MODULES entry {entry!r} is neither a .py file nor a folder"
    )


# Lines of the form ``from <mod> import …`` where <mod> is one of our
# sibling modules. Stripped from each file so the bundle's flat global
# namespace doesn't trip over missing modules. ``from daemon import …``
# is stripped too: there is no ``daemon`` module inside the single-file
# bundle, so any such line (an extracted sibling reaching back for a
# daemon-defined constant like ``DAEMON_VERSION`` at runtime) MUST be
# dropped — the name resolves via the bundle's flat global namespace.
def _mod_name(entry: str) -> str:
    return entry[:-3] if entry.endswith(".py") else entry


# Import-line prefixes to strip: `from <sibling> import …` AND
# `from <pkg>.<sub> import …` (intra-package), for every MODULES entry, plus
# `from daemon import …`. Relative imports (`from .`) are handled in
# _strip_sibling_imports directly (any package __init__ assembling its
# fragments uses them, and the names already live in the flat namespace).
SIBLING_PREFIXES = (
    tuple(f"from {_mod_name(m)} import " for m in MODULES)
    + tuple(f"from {_mod_name(m)}." for m in MODULES)
    + ("from daemon import ",)
)


def _strip_main_block(text: str) -> str:
    """Drop a module's ``if __name__ == "__main__":`` block. Only daemon.py
    (the LAST inlined module, handled separately) keeps its entrypoint. A
    sibling's __main__ would otherwise execute FIRST in the bundle (it inlines
    earlier) and hijack the process — e.g. verify.py's argparse CLI would
    consume the daemon's ``--port/--root`` argv and exit. Indent-based block
    strip, same shape as the TYPE_CHECKING handling above."""
    out: list[str] = []
    skip_indent: int | None = None
    for line in text.splitlines(keepends=True):
        if skip_indent is not None:
            if line.strip() == "":
                continue
            if len(line) - len(line.lstrip()) > skip_indent:
                continue  # block body
            skip_indent = None  # dedented out
        stripped = line.lstrip()
        if stripped.split("#", 1)[0].rstrip() in (
            'if __name__ == "__main__":',
            "if __name__ == '__main__':",
        ):
            skip_indent = len(line) - len(stripped)
            continue
        out.append(line)
    return "".join(out)


def _git_rev() -> str:
    """Short HEAD sha, or 'untracked' outside a git tree (CI / detached)."""
    try:
        return subprocess.check_output(
            ["git", "rev-parse", "--short", "HEAD"], cwd=ROOT, text=True
        ).strip()
    except (subprocess.CalledProcessError, FileNotFoundError):
        return "untracked"


def _header(rev: str) -> str:
    return (
        "#!/usr/bin/env python3\n"
        "# AUTO-GENERATED by daemon/bundle.py — do not edit this file.\n"
        "# Edit daemon/daemon.py (or, post-DM3, the source modules under daemon/)\n"
        "# then re-run `python daemon/bundle.py`.\n"
        f"# Source: {rev}\n"
    )


def _strip_sibling_imports(text: str) -> str:
    """Drop sibling-module import lines + duplicate ``from __future__``
    lines. Python's __future__ imports must appear at the very top of
    a file; the bundle puts one canonical line at the very top and
    every inlined module's local copy is stripped.

    Handles the multi-line form ``from <sibling> import (\\n  A,\\n  B,\\n)``:
    when the drop-prefix line ends with ``(``, every subsequent line is
    dropped until the matching ``)``."""
    drop_prefixes = SIBLING_PREFIXES + ("from __future__ import ",)
    keep: list[str] = []
    skip_until_close = False
    tc_indent: int | None = None  # indent of an `if TYPE_CHECKING:` block being dropped
    for line in text.splitlines(keepends=True):
        if skip_until_close:
            # Look for the closing paren on CODE only — a `)` inside a
            # trailing comment (e.g. `name,  # see foo(bar)`) must NOT be
            # mistaken for the end of the multi-line import, or the real
            # closing `)` + remaining names leak into the bundle unstripped
            # → IndentationError.
            if ")" in line.split("#", 1)[0]:
                skip_until_close = False
            continue
        # Drop `if TYPE_CHECKING:` blocks wholesale — they're type-only
        # scaffolding never executed at runtime, and their bodies often
        # import siblings (`from daemon import Cluster`) which the import
        # strip below would otherwise remove, leaving a dangling `if:`
        # → IndentationError in the bundle.
        if tc_indent is not None:
            if line.strip() == "":
                continue
            cur_indent = len(line) - len(line.lstrip())
            if cur_indent > tc_indent:
                continue  # block body
            tc_indent = None  # dedent → block ended; process this line normally
        stripped = line.lstrip()
        # Match the CODE part only — a trailing comment
        # (`if TYPE_CHECKING:  # note`) must NOT defeat detection, or the
        # block body's sibling imports get stripped leaving a dangling `if:`.
        if stripped.split("#", 1)[0].rstrip() in (
            "if TYPE_CHECKING:",
            "if typing.TYPE_CHECKING:",
        ):
            tc_indent = len(line) - len(stripped)
            continue
        # Intra-package relative imports (`from . import x`, `from .x import y`)
        # — the names are already defined in the flat bundle namespace.
        if stripped.startswith("from .") and " import " in stripped:
            if "(" in line and ")" not in line.split("#", 1)[0]:
                skip_until_close = True
            continue
        if stripped.startswith(drop_prefixes):
            # If this opens a multi-line `from X import (` (has `(` but no
            # matching `)` on the same line, ignoring trailing comments),
            # skip continuation lines until we see the closing `)`.
            if "(" in line and ")" not in line.split("#", 1)[0]:
                skip_until_close = True
            continue
        keep.append(line)
    return "".join(keep)


def _strip_shebang(text: str) -> str:
    """Drop the leading ``#!...`` line if present so the bundle has
    exactly one (the header's)."""
    if text.startswith("#!"):
        return text.split("\n", 1)[1]
    return text


def _extract_version(src_text: str) -> str:
    """Pull the ``DAEMON_VERSION = "py-X.Y.Z"`` value out of the source so
    the bundler can echo it into the bundle HEADER. As of DA-CONST-01,
    DAEMON_VERSION lives in constants.py (a leaf), not daemon.py."""
    m = re.search(r'^DAEMON_VERSION\s*=\s*"([^"]+)"', src_text, re.MULTILINE)
    if not m:
        raise SystemExit("bundle.py: could not find DAEMON_VERSION in constants.py")
    return m.group(1)


def bundle() -> Path:
    """Write the bundled artifact + a tls/ symlink alongside it.

    Order: header → early version marker → each sibling module
    (paths, storage, …) → daemon.py. Each part is
    sibling-import-stripped. The tls/ link mirrors the production
    layout — the daemon's ``_find_tls_bundle()`` looks for
    ``<here>/tls/`` next to its own ``__file__``; the publisher in DM6
    must copy (not link) for the public CDN."""
    DIST.mkdir(parents=True, exist_ok=True)
    src_text = SRC.read_text()
    # DA-CONST-01 — DAEMON_VERSION moved to the leaf constants.py; read it
    # from there for the early 8 KB version marker the VersionWatcher reads.
    version = _extract_version((ROOT / "constants.py").read_text())
    # ── EARLY version marker (py-1.14.10) ──────────────────────────────
    # The VersionWatcher detects new releases by HTTP Range-fetching only
    # the FIRST 8 KB of the published bundle and parsing `^DAEMON_VERSION`.
    # Since the DM3 modularization inlines daemon.py LAST, the canonical
    # `DAEMON_VERSION` assignment sits ~334 KB deep — far past the 8 KB
    # window — so `_fetch_remote_version` returned None and NO cluster
    # ever auto-updated (published stuck at py-1.14.4, field-confirmed
    # 2026-06-13). Echoing the value at the TOP fixes detection for every
    # already-deployed watcher (they read the first 8 KB of THIS file).
    # The canonical assignment (with the full changelog) is still inlined
    # from daemon.py below; Python just reassigns the identical value.
    parts = [
        _header(_git_rev()),
        "from __future__ import annotations\n\n",
        f'DAEMON_VERSION = "{version}"  # early bundle marker for the '
        f"version-watcher 8 KB range-fetch; canonical def inlined below.\n\n",
    ]
    for mod in MODULES:
        for f in _expand_module(mod):
            rel = f.relative_to(ROOT)
            body = f.read_text()
            parts.append(
                f"\n\n# ── inlined from daemon/{rel} (DM3+ bundle) ─────────────────────────\n"
            )
            parts.append(
                _strip_sibling_imports(_strip_main_block(_strip_shebang(body)))
            )
    parts.append(
        "\n\n# ── inlined from daemon/daemon.py — main module ──────────────────────\n"
    )
    parts.append(_strip_sibling_imports(_strip_shebang(src_text)))
    OUT.write_text("".join(parts))
    OUT.chmod(0o755)
    tls_link = DIST / "tls"
    if not tls_link.exists():
        tls_link.symlink_to(ROOT / "tls", target_is_directory=True)
    _sign_bundle(OUT)
    return OUT


def _sign_bundle(out_path: Path) -> None:
    """Ed25519-sign the bundle so clusters can verify it before auto-updating
    (py-1.27.5). Reads the PRIVATE seed from ``daemon/.release-signing-key``
    (gitignored, never on the CDN), writes ``<bundle>.sig`` (base64) next to
    the bundle. Publish BOTH files to the CDN — the daemon fetches
    ``daemon.py.sig`` and verifies it against the pinned public key. A dev
    build with no key is left unsigned (and a key-pinned cluster will refuse
    to auto-update to it — that's the point)."""
    key_file = ROOT / ".release-signing-key"
    if not key_file.exists():
        print(
            "bundle.py: WARNING — daemon/.release-signing-key absent; bundle is "
            "UNSIGNED. Key-pinned clusters will REFUSE to auto-update to it. "
            "(Fine for local dev; NOT for a published release.)",
            file=sys.stderr,
        )
        return
    import base64

    if str(ROOT) not in sys.path:
        sys.path.insert(0, str(ROOT))
    from crypto_ed25519 import ed25519_publickey, ed25519_sign  # noqa: E402

    seed = bytes.fromhex(key_file.read_text().strip())
    pub = ed25519_publickey(seed)
    sig = ed25519_sign(out_path.read_bytes(), seed, pub)
    sig_path = out_path.with_name(out_path.name + ".sig")
    sig_path.write_text(base64.b64encode(sig).decode("ascii") + "\n")
    print(
        f"bundle.py: signed → {sig_path.name} (release pubkey {pub.hex()[:16]}…)",
        file=sys.stderr,
    )


if __name__ == "__main__":
    out = bundle()
    print(f"wrote {out} ({out.stat().st_size:,} bytes)", file=sys.stderr)
