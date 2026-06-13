"""Links + Protocols registries (Standard §13 + §14).

DM-modularize-3 (py-1.14.5): two self-contained registry classes lifted
verbatim from daemon.py. ``LinksRegistry`` parses/validates/serves
``.meshkore/public/links.yaml`` (module → local/prod/version);
``ProtocolsRegistry`` indexes ``.meshkore/protocols/`` (reusable runbooks)
+ their per-day logs. Zero daemon coupling — pure FS + parsing, fed only
a ``Paths``. The YAML emitter (``_emit_links_yaml`` / ``_emit_scalar``)
and ``_split_frontmatter`` travel with them; daemon.py re-imports
``LinksRegistry`` / ``ProtocolsRegistry`` / ``_split_frontmatter``.

Bundler note: imports shared helpers from utils/paths (stripped; resolved
via the flat namespace)."""

from __future__ import annotations

import hashlib
import os
import re
import struct
import threading
from typing import Any, Dict, List, Optional, Tuple

from hub import Hub
from paths import Paths
from utils import _log, parse_simple_yaml


# ───────────────────────────────────────────────────────────────────────
# Links registry — standard §13
#
# .meshkore/public/links.yaml maps each module to where it runs locally,
# where it is deployed in production, and what version is live. The
# daemon parses it on boot and on file change, validates entries, serves
# them at GET /links + /links/<id>, accepts patches at POST /links/<id>,
# and broadcasts `links.updated` on the WebSocket.

_LINKS_PROVIDERS = frozenset(
    {
        "fly",
        "cloudflare-pages",
        "cloudflare-workers",
        "vercel",
        "render",
        "self-hosted",
        "other",
    }
)
_LINKS_BLOCKS = ("local", "prod", "repo")


def _validate_links_block(
    data: Dict[str, Any],
) -> Tuple[List[Dict[str, Any]], List[str]]:
    errs: List[str] = []
    if not isinstance(data, dict):
        return [], ["links.yaml: top level must be a mapping"]
    mods = data.get("modules") or []
    if not isinstance(mods, list):
        return [], ["links.yaml: `modules:` must be a list"]
    out: List[Dict[str, Any]] = []
    for i, m in enumerate(mods):
        if not isinstance(m, dict):
            errs.append(f"links.yaml: modules[{i}] is not a mapping")
            continue
        mid = m.get("id")
        if not isinstance(mid, str) or not mid.strip():
            errs.append(f"links.yaml: modules[{i}] missing string `id`")
            continue
        entry: Dict[str, Any] = {"id": mid.strip()}
        for blk in _LINKS_BLOCKS:
            v = m.get(blk)
            if v is None:
                continue
            if not isinstance(v, dict):
                errs.append(f"links.yaml: {mid}.{blk} must be a mapping")
                continue
            entry[blk] = v
        prov = (m.get("prod") or {}).get("provider")
        if isinstance(prov, str) and prov.strip() and prov not in _LINKS_PROVIDERS:
            errs.append(
                f"links.yaml: {mid}.prod.provider `{prov}` is not in the canonical set (rendered as plain text)"
            )
        if "notes" in m:
            entry["notes"] = m["notes"]
        out.append(entry)
    return out, errs


class LinksRegistry:
    """Loads + watches .meshkore/public/links.yaml; broadcasts on change."""

    POLL_SEC = 3.0

    def __init__(self, paths: Paths, hub: "Hub"):
        self.paths = paths
        self.hub = hub
        self.modules: List[Dict[str, Any]] = []
        self.errors: List[str] = []
        self._mtime: Optional[float] = None
        self._stop = threading.Event()
        self.reload(broadcast=False)
        threading.Thread(target=self._watch_loop, daemon=True).start()

    def _watch_loop(self) -> None:
        while not self._stop.wait(self.POLL_SEC):
            try:
                self.reload(broadcast=True)
            except Exception:
                pass

    def shutdown(self) -> None:
        self._stop.set()

    def reload(self, broadcast: bool = True) -> bool:
        """Reread the file. Returns True if content changed."""
        if not self.paths.links_yaml.exists():
            changed = bool(self.modules) or self._mtime is not None
            self.modules, self.errors, self._mtime = [], [], None
            if changed and broadcast:
                self.hub.broadcast({"type": "links.updated", "modules": []})
            return changed
        try:
            mt = self.paths.links_yaml.stat().st_mtime
        except OSError:
            mt = None
        if mt is not None and mt == self._mtime:
            return False
        try:
            text = self.paths.links_yaml.read_text()
            data = parse_simple_yaml(text)
            mods, errs = _validate_links_block(data)
        except Exception as exc:
            _log(f"links.yaml: parse error — {exc}")
            return False
        self.modules, self.errors, self._mtime = mods, errs, mt
        for e in errs:
            _log(f"links.yaml: {e}")
        if broadcast:
            self.hub.broadcast(
                {"type": "links.updated", "modules": [m["id"] for m in self.modules]}
            )
        return True

    def as_dict(self) -> Dict[str, Any]:
        return {
            "version": 1,
            "modules": self.modules,
            "_errors": self.errors,
        }

    def get(self, mid: str) -> Optional[Dict[str, Any]]:
        for m in self.modules:
            if m["id"] == mid:
                return m
        return None

    def patch(self, mid: str, patch_body: Dict[str, Any]) -> Tuple[bool, str]:
        """Merge `patch_body` (allowed keys: local/prod/repo/notes) into the
        module entry, write the file atomically, broadcast."""
        if not isinstance(patch_body, dict):
            return False, "patch body must be an object"
        allowed = {"local", "prod", "repo", "notes"}
        unknown = set(patch_body) - allowed
        if unknown:
            return False, f"patch keys not allowed: {sorted(unknown)}"
        # Make sure the file exists with a baseline
        if not self.paths.links_yaml.exists():
            self.paths.links_yaml.parent.mkdir(parents=True, exist_ok=True)
            self.paths.links_yaml.write_text("version: 1\nmodules: []\n")
        # Reread → merge → write. We round-trip through parse_simple_yaml +
        # a minimal serializer below so we don't depend on PyYAML.
        try:
            data = parse_simple_yaml(self.paths.links_yaml.read_text())
        except Exception as exc:
            return False, f"current links.yaml unparseable: {exc}"
        mods = data.get("modules")
        if not isinstance(mods, list):
            mods = []
            data["modules"] = mods
        found = None
        for m in mods:
            if isinstance(m, dict) and m.get("id") == mid:
                found = m
                break
        if found is None:
            found = {"id": mid}
            mods.append(found)
        for k, v in patch_body.items():
            if k == "notes":
                found["notes"] = v
            else:
                base = found.get(k) if isinstance(found.get(k), dict) else {}
                if isinstance(v, dict):
                    base.update(v)
                    found[k] = base
                else:
                    found[k] = v
        data["version"] = 1
        tmp = self.paths.links_yaml.with_suffix(".yaml.tmp")
        tmp.write_text(_emit_links_yaml(data))
        os.replace(tmp, self.paths.links_yaml)
        self.reload(broadcast=True)
        return True, "ok"


def _emit_links_yaml(data: Dict[str, Any]) -> str:
    """Tiny serializer for the links.yaml subset we care about. Stdlib-only.
    Preserves block-mapping layout the file template ships with."""
    out: List[str] = [
        "# .meshkore/public/links.yaml — Deployment links registry (standard §13).",
        "# Agent-maintained. Do not check in secrets — URLs only.",
        "",
        f"version: {int(data.get('version') or 1)}",
        "",
        "modules:",
    ]
    mods = data.get("modules") or []
    if not mods:
        out[-1] = "modules: []"
        return "\n".join(out) + "\n"
    for m in mods:
        if not isinstance(m, dict):
            continue
        mid = m.get("id") or "unknown"
        out.append(f"  - id: {mid}")
        for blk in ("local", "prod", "repo"):
            v = m.get(blk)
            if not isinstance(v, dict) or not v:
                continue
            out.append(f"    {blk}:")
            for k, kv in v.items():
                out.append(f"      {k}: {_emit_scalar(kv)}")
        if "notes" in m and m["notes"] not in (None, ""):
            out.append(f"    notes: {_emit_scalar(m['notes'])}")
    return "\n".join(out) + "\n"


def _emit_scalar(v: Any) -> str:
    if v is None:
        return '""'
    if isinstance(v, bool):
        return "true" if v else "false"
    if isinstance(v, (int, float)):
        return str(v)
    s = str(v)
    if s == "" or any(c in s for c in ":#\"'"):
        return '"' + s.replace("\\", "\\\\").replace('"', '\\"') + '"'
    return s


# ───────────────────────────────────────────────────────────────────────
# Protocols registry — standard §14
#
# `.meshkore/protocols/P<N>-<slug>.md` files are reusable runbooks for
# multi-step work. The daemon parses frontmatter on boot and on file
# change, serves the list at /protocols, individual bodies at
# /protocols/<id>, recent run logs at /protocols/<id>/runs, and
# broadcasts `protocols.updated` on the WS.

_PROTOCOL_FILE_RE = re.compile(r"^P(\d+)-[a-z0-9-]+\.md$")
_PROTOCOL_LOG_RE = re.compile(r"^(P\d+)-(\d{4}-\d{2}-\d{2})-[a-z0-9-]+\.md$")


def _split_frontmatter(text: str) -> Tuple[Dict[str, Any], str]:
    if not text.startswith("---"):
        return {}, text
    end = text.find("\n---", 4)
    if end == -1:
        return {}, text
    fm_text = text[4:end]
    body = text[end + 4 :].lstrip("\n")
    return parse_simple_yaml(fm_text), body


class ProtocolsRegistry:
    """Loads + watches .meshkore/protocols/; broadcasts on change."""

    POLL_SEC = 3.0

    def __init__(self, paths: Paths, hub: "Hub"):
        self.paths = paths
        self.hub = hub
        # Each entry: { id, title, scope, status, updated, file, log_count }
        self.protocols: List[Dict[str, Any]] = []
        self._sig: str = ""
        self._stop = threading.Event()
        self.reload(broadcast=False)
        threading.Thread(target=self._watch_loop, daemon=True).start()

    def _watch_loop(self) -> None:
        while not self._stop.wait(self.POLL_SEC):
            try:
                self.reload(broadcast=True)
            except Exception:
                pass

    def shutdown(self) -> None:
        self._stop.set()

    def reload(self, broadcast: bool = True) -> bool:
        sig = self._compute_sig()
        if sig == self._sig and self.protocols:
            return False
        self._sig = sig
        out: List[Dict[str, Any]] = []
        if self.paths.protocols_dir.exists():
            for fp in sorted(self.paths.protocols_dir.glob("P*-*.md")):
                m = _PROTOCOL_FILE_RE.match(fp.name)
                if not m:
                    continue
                try:
                    text = fp.read_text()
                except OSError:
                    continue
                fm, _body = _split_frontmatter(text)
                pid = str(fm.get("id") or f"P{m.group(1)}")
                entry = {
                    "id": pid,
                    "title": str(fm.get("title") or pid),
                    "scope": str(fm.get("scope") or "cluster"),
                    "status": str(fm.get("status") or "draft"),
                    "priority": str(fm.get("priority") or "medium"),
                    "owner": str(fm.get("owner") or ""),
                    "updated": str(fm.get("updated") or ""),
                    "tags": fm.get("tags") or [],
                    "file": fp.name,
                    "log_count": self._count_logs(pid),
                }
                out.append(entry)
        self.protocols = out
        if broadcast:
            self.hub.broadcast(
                {
                    "type": "protocols.updated",
                    "ids": [p["id"] for p in out],
                }
            )
        return True

    def _compute_sig(self) -> str:
        if not self.paths.protocols_dir.exists():
            return ""
        h = hashlib.sha1()
        for fp in sorted(self.paths.protocols_dir.glob("P*-*.md")):
            try:
                st = fp.stat()
                h.update(fp.name.encode())
                h.update(struct.pack(">dq", st.st_mtime, st.st_size))
            except OSError:
                pass
        return h.hexdigest()

    def list(self) -> List[Dict[str, Any]]:
        return list(self.protocols)

    def get(self, pid: str) -> Optional[Dict[str, Any]]:
        pid = pid.strip()
        for fp in sorted(self.paths.protocols_dir.glob(f"{pid}-*.md")):
            if not _PROTOCOL_FILE_RE.match(fp.name):
                continue
            try:
                text = fp.read_text()
            except OSError:
                return None
            fm, body = _split_frontmatter(text)
            return {
                "id": str(fm.get("id") or pid),
                "title": str(fm.get("title") or pid),
                "frontmatter": fm,
                "body": body,
                "file": fp.name,
            }
        return None

    def runs(self, pid: str, limit: int = 50) -> List[Dict[str, Any]]:
        pid = pid.strip()
        if not self.paths.protocols_log.exists():
            return []
        runs: List[Dict[str, Any]] = []
        for month_dir in sorted(self.paths.protocols_log.iterdir(), reverse=True):
            if not month_dir.is_dir():
                continue
            for fp in sorted(month_dir.iterdir(), reverse=True):
                m = _PROTOCOL_LOG_RE.match(fp.name)
                if not m or m.group(1) != pid:
                    continue
                try:
                    text = fp.read_text()
                except OSError:
                    continue
                fm, _ = _split_frontmatter(text)
                runs.append(
                    {
                        "protocol": pid,
                        "date": m.group(2),
                        "file": f"{month_dir.name}/{fp.name}",
                        "outcome": str(fm.get("outcome") or ""),
                        "operator": str(fm.get("operator") or ""),
                        "agent": str(fm.get("agent") or ""),
                        "commit": str(fm.get("commit") or ""),
                    }
                )
                if len(runs) >= limit:
                    return runs
        return runs

    def _count_logs(self, pid: str) -> int:
        if not self.paths.protocols_log.exists():
            return 0
        n = 0
        for month_dir in self.paths.protocols_log.iterdir():
            if not month_dir.is_dir():
                continue
            for fp in month_dir.iterdir():
                m = _PROTOCOL_LOG_RE.match(fp.name)
                if m and m.group(1) == pid:
                    n += 1
        return n
