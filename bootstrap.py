"""bootstrap.py — pre-Daemon process bootstrapping (identity, token,
port selection + sticky registry, cluster.yaml migration).

Extracted from daemon.py (DA-BOOTSTRAP-01, daemon-architecture-v2). These run
BEFORE the Daemon object exists — pure helpers over leaf modules (paths,
constants, http_server, utils) + stdlib. main() + _parse_args stay in
daemon.py (the entrypoint). No daemon backref, no DAEMON_VERSION coupling."""

from __future__ import annotations

from fsatomic import atomic_write_json

import json
import os
import re
import secrets
import socket
from typing import Dict, Optional

from constants import PORT_RANGE, _PORT_REGISTRY_DIR, _PORT_REGISTRY_FILE
from http_server import _port_free
from paths import Paths
from utils import _log


def _hostname_default() -> str:
    return f"{socket.gethostname().split('.')[0]}-py"


def _detect_identity(paths: Paths) -> Optional[str]:
    if not paths.agents_dir.exists():
        return None
    for yml in sorted(paths.agents_dir.glob("*.yaml")):
        return yml.stem
    return None


def _ensure_token(paths: Paths) -> str:
    """Read or freshly mint the architect bearer token."""
    paths.credentials.mkdir(parents=True, exist_ok=True)
    if paths.token_file.exists():
        tok = paths.token_file.read_text().strip()
        if tok:
            return tok
    tok = secrets.token_urlsafe(32)
    paths.token_file.write_text(tok)
    try:
        os.chmod(paths.token_file, 0o600)
    except OSError:
        pass
    _log(f"minted new architect token at {paths.token_file}")
    return tok


def _migrate_cluster_daemon_block(paths: Paths) -> None:
    """Standard v7 §10.4 migration — ensure cluster.yaml has a `daemon:`
    block with auto_update defaulted to true. Existing clusters scaffolded
    under v6 pick up the new behaviour the first time they boot a v7+
    daemon. No-op when the block (or just the field) is already there,
    so operators who set `auto_update: false` keep their preference."""
    yml = paths.cluster_yaml
    if not yml.exists():
        return
    text = yml.read_text()
    # Crude detection — we don't want to round-trip parse + reserialise
    # because our YAML parser is a tiny subset and would drop comments
    # the operator may have added. Just append a block at EOF if absent.
    has_block = re.search(r"(?m)^daemon\s*:", text) is not None
    if has_block:
        # The block exists; check for auto_update inside it. If the
        # operator wrote `daemon:` with sub-keys but not auto_update,
        # leave it alone — they're aware of the section and chose not
        # to set the field, so default applies at read time.
        return
    block = (
        "\n# Standard v7 §10.4 — Daemon self-update. Written automatically by\n"
        "# the v7+ daemon on first boot. Set auto_update: false to require\n"
        "# explicit confirmation for every version update via the V47 modal.\n"
        "daemon:\n"
        "  auto_update: true\n"
        "  auto_update_source: https://architect.meshkore.com/reference/cluster/scripts/daemon.py\n"
    )
    # Ensure there's exactly one newline between the existing tail and our
    # appended block so YAML stays valid (no trailing whitespace gymnastics).
    if not text.endswith("\n"):
        text += "\n"
    yml.write_text(text + block)


def _last_runtime_port(paths: Paths) -> Optional[int]:
    """The port this cluster bound on its previous boot, if recorded."""
    try:
        return int(paths.port_file.read_text().strip())
    except Exception:
        return None


def _registry_read() -> Dict[str, int]:
    """Read the machine-global cluster_id → port map. Missing or corrupt
    file → {} (we'll re-derive a stable assignment from scratch)."""
    try:
        data = json.loads(_PORT_REGISTRY_FILE.read_text())
    except FileNotFoundError:
        return {}
    except Exception as e:
        _log(f"port-registry read failed ({e}); treating as empty")
        return {}
    out: Dict[str, int] = {}
    if isinstance(data, dict):
        for k, v in data.items():
            try:
                out[str(k)] = int(v)
            except (TypeError, ValueError):
                continue
    return out


def _registry_write(mapping: Dict[str, int]) -> None:
    """Persist the map atomically. Best-effort: a write failure just means
    the next boot re-derives the same assignment, so it is never fatal."""
    try:
        _PORT_REGISTRY_DIR.mkdir(parents=True, exist_ok=True)
        atomic_write_json(
            _PORT_REGISTRY_FILE, mapping, sort_keys=True, trailing_newline=True
        )
    except Exception as e:
        _log(f"port-registry write failed ({e}); assignment not persisted")


def _probe_cluster_id(port: int) -> Optional[str]:
    """Best-effort identity of whoever holds `port`. Returns the served
    `cluster_id`, or "" when a socket is held but no reachable meshkore
    daemon answers /health. Used so we NEVER silently steal a sibling's
    live port — only ports owned by a *different* cluster trigger a
    reassignment."""
    import ssl as _ssl
    import urllib.request as _u

    ctx = _ssl.create_default_context()
    ctx.check_hostname = False
    ctx.verify_mode = _ssl.CERT_NONE
    for scheme in ("https", "http"):
        try:
            req = _u.Request(
                f"{scheme}://127.0.0.1:{port}/health",
                headers={"accept": "application/json"},
            )
            with _u.urlopen(req, timeout=1.5, context=ctx) as r:
                body = json.loads(r.read().decode("utf-8", "replace"))
                return str(body.get("cluster_id") or "")
        except Exception:
            continue
    return ""


def _pick_port(
    paths: Paths,
    cluster_id: str,
    cli_override: Optional[int],
    yaml_port: Optional[int],
) -> int:
    """Assign this cluster a STABLE port (py-1.15.0 — anti-drift).

    Resolution order, highest priority first:
      1. explicit ``--port`` from the operator — honoured hard, and it
         rewrites the sticky registry entry so the choice persists.
      2. the sticky registry assignment for this ``cluster_id``
         (``~/.meshkore/ports.json``).
      3. a fresh assignment, seeded from ``cluster.yaml`` ``architect.port``
         or the last ``.meshkore/.runtime/port`` when free, else the
         lowest free port in the range — then claimed + persisted.

    The chosen port is validated before returning: if it's busy AND held
    by a *different* live cluster we refuse to steal it — we reassign this
    cluster to a fresh free port and persist that. If it's held by our OWN
    ``cluster_id`` (a stale/dying instance or a self-update re-exec) we
    return it unchanged and let the bind path reclaim it. This is what
    makes drift impossible: a cluster's port only ever moves when it would
    otherwise collide with a genuinely different live cluster."""
    registry = _registry_read()
    taken_by_others = {p for cid, p in registry.items() if cid != cluster_id}

    def _claim(port: int) -> int:
        # Re-read + merge so a sibling that registered between our read and
        # now isn't clobbered (its key survives; we only set our own).
        latest = _registry_read()
        latest[cluster_id] = port
        _registry_write(latest)
        registry[cluster_id] = port
        return port

    def _lowest_free(avoid: Optional[int] = None) -> int:
        for p in range(PORT_RANGE[0], PORT_RANGE[1] + 1):
            if p == avoid or p in taken_by_others:
                continue
            if _port_free(p):
                return p
        raise SystemExit(
            f"all ports in {PORT_RANGE[0]}-{PORT_RANGE[1]} are busy or "
            f"reserved by sibling clusters; stop a sibling daemon first "
            f"or override with --port"
        )

    def _valid(p: Optional[int]) -> bool:
        return bool(p) and 1024 <= int(p) <= 65535

    # 1. operator override always wins (becomes the new sticky value)
    if _valid(cli_override):
        return _claim(int(cli_override))

    # 2/3. sticky assignment, or a fresh seed
    chosen = registry.get(cluster_id)
    if chosen is None:
        seed: Optional[int] = None
        for cand in (yaml_port, _last_runtime_port(paths)):
            if _valid(cand) and cand not in taken_by_others and _port_free(int(cand)):
                seed = int(cand)
                break
        chosen = _claim(seed if seed is not None else _lowest_free())

    # 4. anti-steal validation — never silently land on another cluster
    if not _port_free(chosen):
        holder = _probe_cluster_id(chosen)
        if holder and holder != cluster_id:
            fresh = _lowest_free(avoid=chosen)
            _log(
                f"port {chosen} held by cluster '{holder}'; reassigning "
                f"'{cluster_id}' → {fresh} (anti-drift, py-1.15.0)"
            )
            chosen = _claim(fresh)
        # else: held by our own stale/re-exec instance, or a non-meshkore
        # listener → keep the sticky port; the bind path (re-exec wait /
        # fast-fail) decides what to do next.
    return chosen
