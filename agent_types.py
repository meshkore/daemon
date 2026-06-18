"""agent_types.py — agent-type resolution: manifest + normalise + slug-implied.

Extracted from prompts.py (daemon-architecture-v2 Phase 3d). Verbatim move;
imported back where used."""

from __future__ import annotations

from typing import Dict, Optional

from agent_prompts import AGENT_PROMPTS


def _agent_manifest(agent_type: str) -> Dict[str, str]:
    """py-1.10.27 — Per-agent platform+model manifest.

    Reads optional `platform` / `model` fields from `AGENT_PROMPTS[agent_type]`
    (falls back to claude-code/auto for any type that doesn't declare them —
    everything ships through Claude Code today, but DeepSeek / Codex / direct
    Anthropic API agents will declare their own values when wired). The
    returned `quota_key` is the persistence + pause-state key used by
    QuotaState — different agent types that share a platform+model share
    a quota pool.

    Future extension: if a single agent_type spans multiple models (e.g.
    a router that picks Claude or DeepSeek per turn), this returns the
    DEFAULT entry; the dispatch path can override per-turn."""
    p = AGENT_PROMPTS.get(agent_type) or AGENT_PROMPTS.get("custom") or {}
    platform = str(p.get("platform") or "claude-code")
    model = str(p.get("model") or "auto")
    return {
        "platform": platform,
        "model": model,
        "quota_key": f"{platform}/{model}",
    }


def _agent_type_normalised(t: Optional[str]) -> str:
    """Return a known agent_type, defaulting to 'custom' if missing/unknown."""
    if not t:
        return "custom"
    t = str(t).strip().lower()
    return t if t in AGENT_PROMPTS else "custom"


def _agent_type_from_conv_slug(conv: str) -> Optional[str]:
    """py-1.10.12 — Infer agent_type from the conv slug pattern.

    The cockpit's `createConv({type: 'roadmap-architect'})` produces
    slugs of shape `roadmap-architect-<5chars>`. The slug is the only
    UNFORGEABLE signal of intent — every other channel (body field,
    conv_meta sidecar, cockpit localStorage) can drift out of sync.

    When the slug carries the type, we treat it as the source of truth
    and force the agent_type to match. Protects against:
      - cockpit JS stuck on a stale bundle that drops `agent_type`
        from the dispatch body
      - cockpit localStorage convMeta that pre-dates an agent type
        being added to the AgentType union
      - sidecar entries written by an older daemon that defaulted
        to 'custom' before the type was registered

    Returns None for slugs with no implied type."""
    if not conv:
        return None
    for prefix, implied in (
        ("roadmap-architect-", "roadmap-architect"),
        ("deploy-", "deploy"),
        ("db-", "db"),
        ("testing-", "testing"),
        ("audit-", "audit"),
        ("docs-", "docs"),
        ("review-", "review"),
    ):
        if conv.startswith(prefix) and implied in AGENT_PROMPTS:
            return implied
    return None
