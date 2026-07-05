"""remotectl.py — the machine-level remote-control token (initiative `master-copilot`, CPL-2).

ONE operator-grade credential per machine — the personal agent's "mando a
distancia" over the whole Architect. It replaces N per-project master tokens:
the shared daemon already routes projects via `X-MeshKore-Project`, so a single
credential + the header is all "distribute messages between projects" needs.

Storage: `<global-ledger-root>/remote-token` (i.e. `~/.meshkore/remote-token`
in production; the test suite's `MESHKORE_GLOBAL_ROOT` redirects it, so the
suite never writes the operator's real home). Machine-level, outside every
repo, mode 600, NON-EXPIRING (32 urlsafe bytes). Lifecycle is operator-driven
from the cockpit: mint-on-first-boot, rotate, delete.

Scope (enforced by the route layer + the ask/poll handlers, NOT here — this
module only owns the secret's storage + comparison):
  - GET /projects, POST /projects (adopt OR create-from-scratch).
  - POST /team/architect-master/ask + GET /team/requests/<rid> on ANY project
    (header-routed), WITHOUT the master needing `exposure: external` — the
    remote token IS the operator. Asks to any OTHER member id → 403.
  - Everything else with the remote token → 401/403.

Three clean credential classes coexist: the PORTAL token (cockpit, full local
control), the per-MEMBER tokens (TEG third-party consumers, one member/one
project), and this REMOTE token (the operator's hand across all projects).
"""

from __future__ import annotations

import hmac
import os
import secrets
from typing import Any, Optional, Tuple

from fsatomic import atomic_write_text
from utils import _iso_now, _log


class RemoteTokenStore:
    """`<global-ledger-root>/remote-token` — a single non-expiring bearer.

    Cheap to construct per call; every write is atomic (tmp+rename) + chmod
    600. Constructed from the machine-global ledger so it resolves to
    `~/.meshkore/remote-token` in production and to the per-test ledger root
    under `MESHKORE_GLOBAL_ROOT` in the suite."""

    FILENAME = "remote-token"

    def __init__(self, ledger: Any) -> None:
        self.path = ledger.root / self.FILENAME

    def get(self) -> Optional[str]:
        try:
            if not self.path.is_file():
                return None
            tok = self.path.read_text(encoding="utf-8").strip()
        except OSError:
            return None
        return tok or None

    def _write(self, token: str) -> None:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        atomic_write_text(self.path, token + "\n", fsync=True)
        try:
            os.chmod(self.path, 0o600)
        except OSError:
            pass

    def mint(self) -> str:
        """Mint a FRESH token (rotate semantics: the old value dies with this
        write). 32 urlsafe random bytes (43 chars)."""
        token = secrets.token_urlsafe(32)
        self._write(token)
        return token

    def ensure(self) -> Tuple[str, bool]:
        """Return (token, minted). Mints one only when absent (first-boot
        idempotency: re-boots never rotate silently)."""
        existing = self.get()
        if existing:
            return existing, False
        return self.mint(), True

    def delete(self) -> bool:
        try:
            if not self.path.is_file():
                return False
            self.path.unlink()
            return True
        except OSError:
            return False

    def matches(self, presented: Optional[str]) -> bool:
        """Constant-time comparison of a presented bearer against the stored
        token. False when no token is minted or the bearer is empty."""
        stored = self.get()
        if not stored or not presented:
            return False
        return hmac.compare_digest(stored, presented)


class RemoteControlMixin:
    """Remote-token lifecycle + HTTP handlers. Inherited by the Daemon so
    `self.global_ledger` resolves the machine-global store. The token is
    MACHINE-level (not per-project) — every handler here ignores the
    X-MeshKore-Project header."""

    def _remote_token_store(self) -> RemoteTokenStore:
        return RemoteTokenStore(self.global_ledger)

    def _remote_token_matches(self, bearer: Optional[str]) -> bool:
        """True iff `bearer` equals the current machine remote-control token.
        The single source of truth the route layer calls to classify a
        request as operator-grade."""
        try:
            return self._remote_token_store().matches(bearer)
        except Exception:  # noqa: BLE001 — auth check must never crash a request
            return False

    def _ensure_remote_token(self) -> None:
        """First-boot mint (idempotent). Logs the PATH once when it mints —
        never the value."""
        try:
            store = self._remote_token_store()
            _token, minted = store.ensure()
            if minted:
                _log(f"minted machine remote-control token at {store.path}")
        except Exception as e:  # noqa: BLE001 — a mint failure must not block boot
            _log(f"remote-control token mint skipped: {e}")

    # ── GET /remote/token (portal-token gated at the route) ──────────────
    def remote_token_get_http(self) -> Tuple[int, dict]:
        """Return the current remote token for the cockpit UI. Portal-gated at
        the route (cockpit/portal only) — never reachable with the remote
        token itself."""
        tok = self._remote_token_store().get()
        if not tok:
            return 404, {"error": "remote_token_absent", "minted": False}
        return 200, {"token": tok, "minted": True}

    # ── POST /remote/token/rotate (portal-token gated) ───────────────────
    def remote_token_rotate_http(self) -> Tuple[int, dict]:
        token = self._remote_token_store().mint()  # old token dies here
        _log("remote-control token rotated")
        return 200, {"token": token, "rotated_at": _iso_now()}

    # ── DELETE /remote/token (portal-token gated) ────────────────────────
    def remote_token_delete_http(self) -> Tuple[int, dict]:
        deleted = self._remote_token_store().delete()
        if deleted:
            _log("remote-control token deleted (revoked)")
        return 200, {"deleted": deleted}
