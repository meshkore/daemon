"""test_clidrivers_claudecode.py — ClaudeCodeDriver catalog (flagship-models).

Pure-logic test, no daemon boot — same style as test_clidrivers_codex.py.
"""

from __future__ import annotations

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

import providers  # noqa: E402
from clidrivers.claudecode import ClaudeCodeDriver  # noqa: E402


def test_anthropic_catalogs_agree():
    """The drift check the comment in claudecode.py points at.

    Three places list Anthropic models: this driver, `providers.py`'s
    provider registry, and the cockpit's `models.ts`. The first two are both
    served by GET /clients — the driver list when no provider is chosen, the
    registry list once one is — so a picker could offer either. They drifted
    for a whole release: the registry stopped at Opus 4.8 and the driver at
    the three aliases, which is how a Fable id became unreachable from the
    daemon side even though the cockpit had one. Same ids, same order.
    """
    driver_ids = [m["id"] for m in ClaudeCodeDriver().models_catalog()]
    registry_ids = [m["id"] for m in providers.provider_models("anthropic")]
    assert sorted(driver_ids) == sorted(registry_ids), (
        f"driver {sorted(driver_ids)} != registry {sorted(registry_ids)}"
    )
    # The flagship stays reachable as a PINNED id (DM-CLI-12: the `fable`
    # alias now exists too), so its presence is the property worth pinning,
    # not just set equality.
    assert "claude-fable-5-1" in driver_ids
    assert "fable" in driver_ids
    assert "claude-opus-5" in driver_ids
