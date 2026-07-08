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
            out.append(
                {
                    "id": driver.id,
                    "label": driver.label,
                    "installed": driver.find_binary() is not None,
                    "authConfigured": driver.auth_configured(),
                    "models": driver.models_catalog(),
                    "efforts": driver.efforts_catalog(),
                }
            )
        return out
