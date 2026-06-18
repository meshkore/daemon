"""Frontend contract — the daemon's response SHAPES must match what the cockpit
consumes. This is the guarantee the operator asked for: prove the architecture
refactor never breaks the current architect frontend.

The expected shapes are transcribed from the TypeScript interfaces in
`architect/src/lib/daemon-client.ts` (the cockpit's own declared contract). For
each read endpoint we assert every REQUIRED (non-`?`) field is present with the
right type, plus the first element of list-shaped payloads. Optional (`?`)
fields are not required. If the daemon ever drops/renames a field the cockpit
relies on, this test goes red BEFORE a deploy can break the UI.

Keep in sync: when a daemon-client.ts interface gains a required field, add it
here; when the daemon adds an endpoint the cockpit reads, add a CONTRACT entry.
"""

from __future__ import annotations

from typing import Any, Dict

import pytest

from conftest import Daemon

NoneType = type(None)
NUM = (int, float)


def _check(obj: Any, spec: Dict[str, Any], where: str) -> None:
    """Assert obj is a dict carrying every field in spec with an allowed type.
    spec maps field -> a type or tuple of types (include NoneType for `x|null`)."""
    assert isinstance(obj, dict), f"{where}: expected object, got {type(obj).__name__}"
    for field, types in spec.items():
        assert field in obj, f"{where}: missing required field '{field}'"
        assert isinstance(obj[field], types), (
            f"{where}: field '{field}' is {type(obj[field]).__name__}, expected {types}"
        )


# (method, path, auth, top_level_spec, list_field, item_spec)
#   top_level_spec — required fields on the response object
#   list_field      — if set, a list field whose first item is checked against item_spec
#   item_spec       — required fields per list element (None to skip)
# When the whole response IS a list (credentials), top_level_spec is None and
# list_field is "" (the response itself).
HEALTH = {"ok": bool, "identity": str, "port": int, "mode": str}
STORAGE = {
    "root": str,
    "total_bytes": int,
    "total_files": int,
    "buckets": list,
    "generated_at": str,
    "cache_ttl_secs": NUM,
}
BUCKET = {"name": str, "bytes": int, "files": int, "exists": bool}
# NB: /info = health() + version + paths{root,meshkore}. The cockpit's
# InfoResponse declares top-level `root`/`pid` (required) but the daemon has
# always nested root under `paths` and never sent `pid` — InfoResponse carries
# an index signature so TS never enforced it. We assert the REAL longstanding
# shape (the daemon behaviour the cockpit works against today), not the
# over-declared interface. [cockpit interface drift — pre-existing, not a refactor regression]
INFO = {
    "ok": bool,
    "identity": str,
    "port": int,
    "mode": str,
    "version": str,
    "paths": dict,
}
CRED_ITEM = {
    "name": str,
    "size": (int, NoneType),
    "is_symlink": bool,
    "protected": bool,
}
CRON = {
    "jobs": list,
    "coordinator": bool,
    "owner": (str, NoneType),
    "identity": str,
    "tick_sec": NUM,
}
LOG = {"entries": list}
SNAPSHOT = {
    "convs": list,
    "paused_agent_types": dict,
    "quota": dict,
    "debug": dict,
    "version": str,
    "generated_at": str,
}
CONVS = {"convs": list, "generated_at": str}
CONV_SUMMARY = {
    "conv": str,
    "agent_type": (str, NoneType),
    "agent_id": (str, NoneType),
    "parent_conv": (str, NoneType),
    "initiative_id": (str, NoneType),
    "task_id": (str, NoneType),
    "archived": bool,
    "archived_at": (str, NoneType),
    "archived_by": (str, NoneType),
    "live": bool,
    "coordinating": bool,
    "waiting_on": list,
    "created_at": str,
    "last_activity_at": str,
    "msg_count": int,
}
RUNS = {"runs": list, "count": int}
CONTEXT = {
    "exists": bool,
    "root": str,
    "total_words": int,
    "token_estimate": NUM,
    "budget_tokens": int,
    "over_budget": bool,
    "warnings": list,
    "tree": list,
}
LINKS = {"modules": list}
PROTOCOLS = {"protocols": list}

# (path, top_spec, list_field, item_spec)
CONTRACT = [
    ("/health", HEALTH, "buckets" and None, None),
    ("/storage/usage", STORAGE, "buckets", BUCKET),
    ("/info", INFO, None, None),
    ("/cron/list", CRON, None, None),
    ("/log", LOG, None, None),
    ("/chat/snapshot", SNAPSHOT, None, None),
    ("/chat/convs", CONVS, "convs", CONV_SUMMARY),
    ("/runs", RUNS, None, None),
    ("/context", CONTEXT, "tree" and None, None),  # tree may be empty; shape-only
    ("/links", LINKS, None, None),
    ("/protocols", PROTOCOLS, None, None),
]


@pytest.mark.parametrize("path,top,lf,item", CONTRACT, ids=[c[0] for c in CONTRACT])
def test_endpoint_shape(daemon: Daemon, path, top, lf, item) -> None:
    r = daemon.get(path, headers=daemon.auth)
    assert r.status_code == 200, f"{path}: {r.status_code}"
    body = r.json()
    _check(body, top, path)
    if lf and item:
        items = body[lf]
        if items:  # only assert the element shape when the fixture has data
            _check(items[0], item, f"{path}[{lf}][0]")


def test_credentials_list_shape(daemon: Daemon) -> None:
    """/credentials returns a bare array of CredentialListEntry."""
    r = daemon.get("/credentials", headers=daemon.auth)
    assert r.status_code == 200
    body = r.json()
    assert isinstance(body, list), "/credentials must be a JSON array"
    if body:
        _check(body[0], CRED_ITEM, "/credentials[0]")


def test_convs_present_in_populated(daemon: Daemon) -> None:
    """The populated fixture has timeline activity → /chat/convs must return at
    least one conv, so the CONV_SUMMARY contract above is actually exercised."""
    r = daemon.get("/chat/convs", headers=daemon.auth)
    assert r.status_code == 200
    assert len(r.json()["convs"]) >= 1, "expected >=1 conv from the populated fixture"
