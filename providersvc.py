"""providersvc.py — machine-global provider config + secret store + the
`/config/providers` HTTP surface (initiative `multi-provider-agents`, MPV1).

Split mirrors remotectl.py: a tiny STORE class owns the secret on disk,
and a Mixin (inherited by the Daemon) owns resolution + HTTP. All state
is MACHINE-global (`self.global_ledger`), not per-project — the
X-MeshKore-Project header is irrelevant here.

Three data stores:
  - NON-secret config (enabled clients, per-provider base_url/small_model/
    enabled) → GlobalLedger.clients-config.json.
  - Provider API KEYS → `<credentials_dir>/provider-<id>.key`, chmod 0600,
    atomic. NEVER returned by any list endpoint (only a `keyPresent` bool);
    NEVER in the frontend / localStorage / git.
  - The static provider registry (labels, catalogs, defaults, whether a
    key is required) → providers.py.

The frontend tells the daemon WHICH provider to use; `resolve_provider`
supplies the key + endpoint at spawn time (runnerspawn.build_launch_env).
"""

from __future__ import annotations

import os
import re
from typing import Any, Dict, Optional, Tuple

from fsatomic import atomic_write_text
from providers import (
    CLIENT_KEY_SPECS,
    PROVIDERS,
    known_client_key_ids,
    known_provider_ids,
    provider_for,
)
from utils import _log

_ID_RE = re.compile(r"^[a-z][a-z0-9-]{0,31}$")


class ProviderKeyStore:
    """`<global-ledger-credentials>/provider-<id>.key` — one bearer per
    provider, machine-global, mode 0600, atomic write. Cheap to construct
    per call (mirrors remotectl.RemoteTokenStore / credapi.credential_write)."""

    def __init__(self, ledger: Any) -> None:
        self.dir = ledger.credentials_dir

    def _path(self, provider_id: str):
        return self.dir / f"provider-{provider_id}.key"

    def get(self, provider_id: str) -> Optional[str]:
        try:
            p = self._path(provider_id)
            if not p.is_file():
                return None
            val = p.read_text(encoding="utf-8").strip()
        except OSError:
            return None
        return val or None

    def present(self, provider_id: str) -> bool:
        return bool(self.get(provider_id))

    def set(self, provider_id: str, value: str) -> None:
        self.dir.mkdir(parents=True, exist_ok=True)
        try:
            os.chmod(self.dir, 0o700)
        except OSError:
            pass
        p = self._path(provider_id)
        atomic_write_text(p, value.strip() + "\n", fsync=True)
        try:
            os.chmod(p, 0o600)
        except OSError:
            pass

    def delete(self, provider_id: str) -> bool:
        try:
            p = self._path(provider_id)
            if not p.is_file():
                return False
            p.unlink()
            return True
        except OSError:
            return False


class ProvidersMixin:
    """Provider resolution + config HTTP. Inherited by the Daemon so
    `self.global_ledger` resolves the machine-global store."""

    def _provider_key_store(self) -> ProviderKeyStore:
        return ProviderKeyStore(self.global_ledger)

    # ── resolution (used by runnerspawn.build_launch_env) ────────────────
    def resolve_provider(self, provider_id: Optional[str]) -> Dict[str, Any]:
        """Resolve a provider id → {id, base_url, auth_token,
        small_fast_model, requires_key, enabled, available}. Reads the
        machine-global config + the key file. `auth_token` is the ONLY
        secret in the dict and is used strictly to build the launch env —
        never broadcast or logged."""
        meta = provider_for(provider_id)
        pid = str(meta["id"])
        cfg = self.global_ledger.load_clients_config()
        pcfg = (cfg.get("providers") or {}).get(pid) or {}
        base_url = str(
            pcfg.get("base_url") or meta.get("default_base_url") or ""
        ).strip()
        small = str(
            pcfg.get("small_fast_model") or meta.get("default_small_model") or ""
        ).strip()
        enabled = pcfg.get("enabled", True) is not False
        requires_key = bool(meta.get("requires_key"))
        key = self._provider_key_store().get(pid)
        available = enabled and (not requires_key or bool(key))
        return {
            "id": pid,
            "base_url": base_url,
            "auth_token": key,
            "small_fast_model": small,
            "requires_key": requires_key,
            "enabled": enabled,
            "available": available,
        }

    def provider_available(self, provider_id: Optional[str]) -> bool:
        try:
            return bool(self.resolve_provider(provider_id).get("available"))
        except Exception:  # noqa: BLE001 — availability probe must not crash a spawn
            return False

    # ── client-level API keys (Codex/Gemini) ─────────────────────────────
    def resolve_client_key(self, client_id: str) -> Optional[str]:
        """The daemon-managed API key for a non-claude-code CLIENT (codex,
        gemini), or None when absent — the client's own native login/env
        then applies unchanged. Used by runnerspawn to inject the right
        env var (`providers.CLIENT_KEY_SPECS[client_id]['env_var']`) only
        when a key is actually stored."""
        if client_id not in CLIENT_KEY_SPECS:
            return None
        return self._provider_key_store().get(client_id)

    # ── public listing (no secrets) — embedded in GET /clients ───────────
    def providers_public_listing(self) -> list:
        """[{id, label, requiresKey, available, defaultModel, models}] for
        every provider — the availability list the cockpit's member UI reads
        to gate the Provider dropdown. Carries NO key material."""
        out = []
        for pid in known_provider_ids():
            meta = PROVIDERS[pid]
            r = self.resolve_provider(pid)
            models = list(meta.get("models") or [])
            out.append(
                {
                    "id": pid,
                    "label": meta.get("label") or pid,
                    "requiresKey": bool(meta.get("requires_key")),
                    "available": bool(r.get("available")),
                    "defaultModel": (models[0]["id"] if models else None),
                    "models": models,
                }
            )
        return out

    # ── unified auth-slot listing ─────────────────────────────────────────
    #
    # ONE list for the Config → General settings UI, covering every
    # daemon-managed AI credential: claude-code's own providers (Anthropic —
    # no key; ZAI — key + swappable base-url/small-model) PLUS the other
    # CLIENTS that authenticate via a single stored key (Codex, Gemini — no
    # base-url swap, each only ever talks to its own vendor's API). Kept to
    # a handful of entries by design (today: 4) rather than a sprawling list.
    def _auth_slot_ids(self) -> list:
        return [*known_provider_ids(), *known_client_key_ids()]

    def _auth_slot_config_entry(
        self, slot_id: str, providers_cfg: Dict[str, Any]
    ) -> Dict[str, Any]:
        """One row of the unified list. `hasEndpoint` tells the cockpit
        whether to render the base-url/small-model inputs (only ZAI-like
        claude-code providers swap an endpoint; Codex/Gemini don't)."""
        keystore = self._provider_key_store()
        if slot_id in PROVIDERS:
            meta = PROVIDERS[slot_id]
            r = self.resolve_provider(slot_id)
            requires_key = bool(meta.get("requires_key"))
            return {
                "id": slot_id,
                "label": meta.get("label") or slot_id,
                "requiresKey": requires_key,
                "hasEndpoint": requires_key,  # only zai-like entries today
                "enabled": bool(r.get("enabled")),
                "baseUrl": r.get("base_url") or "",
                "smallFastModel": r.get("small_fast_model") or "",
                "keyPresent": keystore.present(slot_id),
                "available": bool(r.get("available")),
                "models": list(meta.get("models") or []),
            }
        spec = CLIENT_KEY_SPECS[slot_id]
        pcfg = providers_cfg.get(slot_id) or {}
        enabled = pcfg.get("enabled", True) is not False
        key_present = keystore.present(slot_id)
        return {
            "id": slot_id,
            "label": spec.get("label") or slot_id,
            "requiresKey": True,
            "hasEndpoint": False,
            "enabled": enabled,
            "baseUrl": "",
            "smallFastModel": "",
            "keyPresent": key_present,
            # Codex/Gemini also work via their own native login (`codex
            # login` / `gcloud auth`) — the daemon key is an optional
            # convenience, so "available" doesn't require it, unlike ZAI.
            "available": enabled,
            "models": [],
        }

    # ── GET /config/providers (portal-gated) ─────────────────────────────
    def provider_config_get_http(self) -> Tuple[int, Dict[str, Any]]:
        """Full machine-global config for the Config → General settings UI.
        Every entry carries a `keyPresent` boolean but NEVER the key
        itself."""
        cfg = self.global_ledger.load_clients_config()
        providers_cfg = cfg.get("providers") or {}
        providers = [
            self._auth_slot_config_entry(slot_id, providers_cfg)
            for slot_id in self._auth_slot_ids()
        ]
        return 200, {"providers": providers}

    # ── POST /config/providers (portal-gated) ────────────────────────────
    def provider_config_set_http(self, body: Any) -> Tuple[int, Dict[str, Any]]:
        """Apply a partial update. Body:
            {
              "providers": {"<id>": {
                    "enabled": bool, "base_url": str, "small_fast_model": str,
                    "key": str,          # set/replace the API key (omit to keep)
                    "clear_key": bool    # true → delete the stored key
              }, ...}
            }
        `<id>` is any of Anthropic/ZAI (claude-code providers) or Codex/
        Gemini (client keys) — one unified id space. `base_url`/
        `small_fast_model` are accepted but ignored for entries that don't
        support them (Codex/Gemini). Unknown ids are ignored. Keys are
        written to the chmod-600 credentials store, NEVER into
        clients-config.json."""
        if not isinstance(body, dict):
            return 400, {"error": "JSON object body required"}
        cfg = self.global_ledger.load_clients_config()
        providers_cfg = dict(cfg.get("providers") or {})
        keystore = self._provider_key_store()
        known_slots = set(self._auth_slot_ids())

        for slot_id, patch in (body.get("providers") or {}).items():
            if slot_id not in known_slots or not isinstance(patch, dict):
                continue
            entry = dict(providers_cfg.get(slot_id) or {})
            if "enabled" in patch:
                entry["enabled"] = bool(patch["enabled"])
            if "base_url" in patch:
                entry["base_url"] = str(patch.get("base_url") or "").strip()
            if "small_fast_model" in patch:
                entry["small_fast_model"] = str(
                    patch.get("small_fast_model") or ""
                ).strip()
            providers_cfg[slot_id] = entry
            # Secret handling — NEVER persisted into the config file.
            if patch.get("clear_key"):
                if keystore.delete(slot_id):
                    _log(f"providers: cleared API key for {slot_id}")
            else:
                key = patch.get("key")
                if isinstance(key, str) and key.strip():
                    keystore.set(slot_id, key)
                    _log(f"providers: stored API key for {slot_id} (0600)")

        self.global_ledger.save_clients_config({"providers": providers_cfg})
        return self.provider_config_get_http()
