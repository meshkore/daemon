"""cluster.py — Cluster (cluster.yaml + roadmap frontmatter) + frontmatter helpers.

Extracted from daemon.py (DA-CLUSTER-01, daemon-architecture-v2). Owns
parsing cluster.yaml, the modules/initiatives/tasks frontmatter read+patch
(_patch_frontmatter), and status normalization. Pure config/IO over Paths +
the utils YAML/frontmatter helpers — no daemon backref. Consumers
(prompts/runner/cron/daemon) type-ref Cluster only.
"""

from __future__ import annotations

import os
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

from paths import Paths
from utils import _FM_RE, _log, parse_simple_yaml


class Cluster:
    def __init__(self, paths: Paths):
        self.paths = paths
        self.data: Dict[str, Any] = {}
        # Cron scheduler (D-CRON-01): validated job set + ownership.
        # Populated by reload(); empty + None until a `crons:` block
        # appears in cluster.yaml.
        self.crons: List[Dict[str, Any]] = []
        self.crons_owner: Optional[str] = None
        self.reload()

    def reload(self) -> None:
        if not self.paths.cluster_yaml.exists():
            raise SystemExit(
                f"\n .meshkore/public/cluster.yaml not found at {self.paths.cluster_yaml}."
                "\n   Run `meshcore init` (or hand-author cluster.yaml from"
                "\n   https://meshkore.com/reference/cluster/templates/) and re-run.\n"
            )
        self.data = parse_simple_yaml(self.paths.cluster_yaml.read_text())
        # Validate the cron block last so a bad config logs warnings but
        # never blocks the daemon's other features.
        self.crons, errs = _validate_crons_block(self.data)
        for e in errs:
            _log(f"cluster.yaml crons: {e}")
        owner = self.data.get("crons_owner")
        self.crons_owner = (
            owner.strip() if isinstance(owner, str) and owner.strip() else None
        )
        if self.crons and not self.crons_owner:
            _log(
                "cluster.yaml has crons: but no crons_owner — scheduler will tick but never fire"
            )

    @property
    def id(self) -> str:
        return str(self.data.get("id") or "unknown")

    @property
    def name(self) -> str:
        return str(self.data.get("name") or self.id)

    @property
    def type(self) -> str:
        return str(self.data.get("type") or "dev")

    @property
    def architect_port(self) -> Optional[int]:
        # cluster.yaml.architect.port (preferred) → fall back to legacy portal.port
        for key in ("architect", "portal"):
            sec = self.data.get(key)
            if isinstance(sec, dict) and "port" in sec:
                try:
                    return int(sec["port"])
                except (TypeError, ValueError):
                    pass
        return None

    @property
    def modules(self) -> List[Dict[str, Any]]:
        m = self.data.get("modules") or []
        return m if isinstance(m, list) else []


def _patch_frontmatter(fp: "Path", patch: Dict[str, Any]) -> bool:
    """Idempotent frontmatter merge. Writes only the fields in `patch`
    that differ from current. Preserves field order: known fields keep
    their position, new fields append in `patch` order.

    py-1.12.4 — a `None` value in the patch REMOVES that key from the
    frontmatter (used by the bidirectional reconcile to wipe stale
    `completed_at` / `commit_sha` when a partially-done initiative is
    reverted from done → active).

    Returns True iff the file was actually rewritten."""
    text = fp.read_text(errors="replace")
    m = _FM_RE.match(text)
    if not m:
        # No frontmatter to patch — refuse rather than corrupt.
        return False
    fm_block = m.group(1)
    rest = text[m.end() :]
    current = parse_simple_yaml(fm_block)
    # Detect any actual change. A None patch entry counts as a change
    # iff the key currently exists.
    changed = False
    for k, v in patch.items():
        if v is None:
            if k in current and current.get(k) not in (None, ""):
                changed = True
                break
        else:
            if str(current.get(k) or "") != str(v):
                changed = True
                break
    if not changed:
        return False
    lines = fm_block.splitlines()
    handled: set[str] = set()
    new_lines: List[str] = []
    for line in lines:
        if ":" in line and not line.startswith((" ", "\t", "-", "#")):
            key = line.split(":", 1)[0].strip()
            if key in patch:
                handled.add(key)
                if patch[key] is None:
                    # Skip the line — that's the removal.
                    continue
                new_lines.append(f"{key}: {patch[key]}")
                continue
        new_lines.append(line)
    for k, v in patch.items():
        if k in handled or v is None:
            continue
        new_lines.append(f"{k}: {v}")
    new_fm = "\n".join(new_lines)
    if not new_fm.endswith("\n"):
        new_fm += "\n"
    new_text = "---\n" + new_fm + "---\n" + rest.lstrip("\n")
    tmp = fp.with_suffix(fp.suffix + ".tmp")
    tmp.write_text(new_text)
    os.replace(tmp, fp)
    return True


def normalize_status(s: Any) -> str:
    s = str(s or "backlog").lower()
    if s in ("in_progress", "in-progress"):
        return "active"
    if s in ("backlog", "next", "active", "blocked", "done"):
        return s
    return "backlog"


# ── cluster.yaml `crons:` validation (DA-CLUSTER-01, moved from daemon.py) ──

_CRON_RESTART_POLICIES = frozenset({"never", "on-failure", "always"})

_CRON_DEFAULTS = {
    "enabled": True,
    "max_runtime_sec": 7200,  # 2h
    "restart_policy": "never",
    "retention_runs": 30,
    "destructive": False,
}


def _validate_cron_expr(expr: str) -> Optional[str]:
    """Lightweight validation. Full parsing lands in D-CRON-02. Here we
    only need to reject obviously malformed values at config load so the
    daemon doesn't carry junk into the scheduler later.

    Returns None on OK, or a short error message string on reject.
    Accepts 5 space-separated fields. Each field is non-empty and
    consists of characters from [0-9*/,\\-]. Quartz (6 fields with
    seconds), `@daily`-style aliases, and the `L/W/#` modifiers are
    explicitly NOT supported in v1.
    """
    if not isinstance(expr, str) or not expr.strip():
        return "schedule must be a non-empty string"
    fields = expr.strip().split()
    if len(fields) != 5:
        return f"schedule must have 5 space-separated fields, got {len(fields)}"
    allowed = set("0123456789*/,-")
    for i, f in enumerate(fields):
        if not f:
            return f"schedule field {i} is empty"
        if not set(f).issubset(allowed):
            return f"schedule field {i} ({f!r}) contains unsupported characters"
    return None


def _validate_crons_block(
    data: Dict[str, Any],
) -> Tuple[List[Dict[str, Any]], List[str]]:
    """Validates the `crons:` section of cluster.yaml in isolation.
    Returns (cleaned_jobs, errors). Bad entries are skipped (not raised)
    so a single broken job doesn't disable the entire scheduler.

    Each returned job has defaults filled in and the schema's shape
    enforced. Invariants:
      - id is a non-empty kebab-case string, unique within the list
      - cmd is non-empty string
      - schedule passes _validate_cron_expr
      - restart_policy is in _CRON_RESTART_POLICIES
      - env values are strings
    """
    raw = data.get("crons") or []
    if not isinstance(raw, list):
        return [], ["crons: must be a list"]
    out: List[Dict[str, Any]] = []
    errors: List[str] = []
    seen_ids: set = set()
    for idx, entry in enumerate(raw):
        if not isinstance(entry, dict):
            errors.append(f"crons[{idx}] is not a dict — skipped")
            continue
        cid = entry.get("id")
        if not isinstance(cid, str) or not cid.strip():
            errors.append(f"crons[{idx}] missing id — skipped")
            continue
        cid = cid.strip()
        if cid in seen_ids:
            errors.append(f"crons[{idx}] duplicate id {cid!r} — skipped")
            continue
        cmd = entry.get("cmd")
        if not isinstance(cmd, str) or not cmd.strip():
            errors.append(f"crons[{cid}] missing cmd — skipped")
            continue
        sched = entry.get("schedule")
        sched_err = (
            _validate_cron_expr(sched) if isinstance(sched, str) else "schedule missing"
        )
        if sched_err:
            errors.append(f"crons[{cid}] {sched_err} — skipped")
            continue
        policy = entry.get("restart_policy", _CRON_DEFAULTS["restart_policy"])
        if policy not in _CRON_RESTART_POLICIES:
            errors.append(
                f"crons[{cid}] restart_policy={policy!r} not in "
                f"{sorted(_CRON_RESTART_POLICIES)} — defaulting to 'never'"
            )
            policy = "never"
        env = entry.get("env") or {}
        if not isinstance(env, dict):
            errors.append(f"crons[{cid}] env must be a dict — replaced with empty")
            env = {}
        env_clean: Dict[str, str] = {}
        for k, v in env.items():
            if not isinstance(k, str) or not isinstance(v, str):
                errors.append(
                    f"crons[{cid}] env {k!r}: values must be strings — dropped"
                )
                continue
            env_clean[k] = v

        cleaned = {
            "id": cid,
            "name": str(entry.get("name") or cid),
            "schedule": sched.strip(),
            "cmd": cmd.strip(),
            "cwd": entry.get("cwd"),
            "env": env_clean,
            "enabled": bool(entry.get("enabled", _CRON_DEFAULTS["enabled"])),
            "max_runtime_sec": int(
                entry.get("max_runtime_sec", _CRON_DEFAULTS["max_runtime_sec"])
            ),
            "restart_policy": policy,
            "retention_runs": int(
                entry.get("retention_runs", _CRON_DEFAULTS["retention_runs"])
            ),
            "destructive": bool(
                entry.get("destructive", _CRON_DEFAULTS["destructive"])
            ),
        }
        out.append(cleaned)
        seen_ids.add(cid)
    return out, errors
