"""clidriverssvc.py — GET /clients wire layer (DM-CLI-06, multi-cli-clients).

ClientsMixin is inherited by Daemon so `daemon.clients_listing()`
resolves on the combined instance, mirroring `readapi.py:agents_listing`
and `teamsvc.py`'s `<resource>_<verb>_http` naming convention (this one
has no HTTP verb suffix since it's the only method — a single GET, no
mutation surface).

The in-process data layer (driver registry, per-driver catalogs) lives
in `clidrivers/` (DM-CLI-01/04/05); this module is the thin WIRE layer
that turns it into the JSON shape the cockpit fetches."""

from __future__ import annotations

from typing import Any, Dict, List

from clidrivers import DRIVERS
from providers import CLIENT_KEY_SPECS


class ClientsMixin:
    def clients_listing(self) -> List[Dict[str, Any]]:
        """[{id, label, installed, authConfigured, models, efforts}]
        for every registered CLI-client driver. `installed`/
        `authConfigured` are cheap LOCAL probes (shutil.which + env/
        credentials-file presence) computed fresh on every call — never
        cached, so this always reflects the actual state of the
        machine the daemon is running on right now, not a stale
        snapshot from boot time."""
        out: List[Dict[str, Any]] = []
        for driver_id in sorted(DRIVERS):
            driver = DRIVERS[driver_id]
            entry: Dict[str, Any] = {
                "id": driver.id,
                "label": driver.label,
                "installed": driver.find_binary() is not None,
                "authConfigured": driver.auth_configured(),
                "models": driver.models_catalog(),
                "efforts": driver.efforts_catalog(),
            }
            # multi-provider-agents (MPV1) — the claude-code client carries a
            # PROVIDER dimension (Anthropic / ZAI / …). Attach the availability
            # list (booleans + catalogs, NEVER key material) so the cockpit's
            # member UI can gate the Provider dropdown. Older cockpits ignore
            # the extra field; daemons without ProvidersMixin never reach here.
            if driver.id == "claude-code" and hasattr(self, "providers_public_listing"):
                entry["providers"] = self.providers_public_listing()
            # multi-provider-agents follow-up — Codex/Gemini aren't claude-code
            # providers; they're CLIENTS with an optional daemon-managed API
            # key (see providers.CLIENT_KEY_SPECS). Surface whether one is
            # stored, and let it upgrade `authConfigured` from the driver's
            # own env-only probe (False/None) to a confident True — a key set
            # in Config → General settings makes the client usable even in a
            # shell with no env var / no interactive login done.
            elif driver.id in CLIENT_KEY_SPECS and hasattr(self, "resolve_client_key"):
                key = self.resolve_client_key(driver.id)
                entry["keyPresent"] = key is not None
                if key is not None:
                    entry["authConfigured"] = True
            out.append(entry)
        return out
