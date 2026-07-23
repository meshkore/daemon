"""providers.py — LLM provider registry for the claude-code client
(initiative `multi-provider-agents`, MPV1).

A "provider" is the backend a `claude-code` turn talks to. Claude Code
speaks the Anthropic Messages API, and it will speak it against ANY
compatible endpoint if the environment points it there — so switching
provider is a pure ENV change at `subprocess.Popen(env=…)` time, applied
PER INSTANCE:

    ANTHROPIC_BASE_URL          the provider's Anthropic-compatible endpoint
    ANTHROPIC_AUTH_TOKEN        the provider's API key (bearer)
    ANTHROPIC_MODEL             the model id (also passed via --model)
    ANTHROPIC_SMALL_FAST_MODEL  the light model for background tasks

- `anthropic` (DEFAULT): native login/config — NO base-url / token
  override. We still SCRUB any stray `ANTHROPIC_BASE_URL`/
  `ANTHROPIC_AUTH_TOKEN` inherited from the daemon's own shell so an
  Anthropic turn can never leak onto a custom endpoint (and vice versa).
- `zai`: GLM models via ZAI's Anthropic-compatible endpoint. Base-url +
  key come from the machine-global config (`providersvc.resolve_provider`),
  NEVER hardcoded here (the plan/URL can change).

This is ORTHOGONAL to `client` (the CLI binary) and to `model`/`effort`.
It is meaningful only for `client == claude-code`; other clients ignore
it. Adding a provider is one `PROVIDERS` entry — no plumbing changes.

Pure module: no daemon/globalledger imports (so it bundles before
team.py, which imports `known_provider_ids`, and stays trivially
testable). The resolved secrets are passed IN by the caller.
"""

from __future__ import annotations

from typing import Any, Dict, List, Optional

DEFAULT_PROVIDER = "anthropic"

# The Anthropic-family env vars claude-code reads to pick its endpoint /
# credentials. We manage ALL of them per launch so a value inherited from
# the daemon's own environment can never silently route a turn to the
# wrong backend. `ANTHROPIC_API_KEY` is scrubbed only on the ZAI path
# (see build_launch_env) — on the anthropic path we leave it intact so an
# operator relying on a key-based native login keeps working.
_CROSS_KEYS = ("ANTHROPIC_BASE_URL", "ANTHROPIC_AUTH_TOKEN")

# ZAI's Anthropic-compatible base URL. This is only the FROM-SCRATCH
# default seeded into the config; the live value is always read back from
# the machine-global config so the operator can change it per plan.
ZAI_DEFAULT_BASE_URL = "https://api.z.ai/api/anthropic"
ZAI_DEFAULT_SMALL_MODEL = "glm-4.5-air"


# id → static metadata. `models` mirrors the cockpit's lib/models.ts
# PROVIDER_CATALOG (both sides read a static catalog; upstream model
# lists are not fetched live — updated via normal version bumps).
PROVIDERS: Dict[str, Dict[str, Any]] = {
    "anthropic": {
        "id": "anthropic",
        "label": "Anthropic",
        "requires_key": False,
        "default_base_url": "",
        "default_small_model": "",
        "models": [
            {"id": "opus", "label": "Opus (latest)"},
            {"id": "sonnet", "label": "Sonnet (latest)"},
            {"id": "haiku", "label": "Haiku (latest)"},
            {"id": "claude-opus-4-8", "label": "Opus 4.8"},
            {"id": "claude-sonnet-5", "label": "Sonnet 5"},
            {"id": "auto", "label": "Auto"},
        ],
    },
    "zai": {
        "id": "zai",
        "label": "ZAI (GLM)",
        "requires_key": True,
        "default_base_url": ZAI_DEFAULT_BASE_URL,
        "default_small_model": ZAI_DEFAULT_SMALL_MODEL,
        "models": [
            {"id": "glm-4.6", "label": "GLM-4.6"},
            {"id": "glm-4.5-air", "label": "GLM-4.5 Air"},
        ],
    },
}


def known_provider_ids() -> List[str]:
    return sorted(PROVIDERS.keys())


# ── client-level API keys (follow-up, same initiative) ──────────────────
#
# Codex and Gemini are not "providers" of claude-code — they ARE the
# client, each talking only to its own vendor's API. There is no base-url
# swap here, just an optional daemon-managed API key so headless agents
# don't depend on an interactive `codex login` / `gcloud auth` having
# already happened in that shell. Absent a stored key, the client's own
# native login/env keeps working exactly as before (zero behavior change).
# Reuses the SAME chmod-0600 store as providers (providersvc.ProviderKeyStore
# is generic over any id) — shown alongside Anthropic/ZAI in one "Providers"
# list in Config → General settings so the operator has ONE place for every
# daemon-managed AI credential (capped at a handful, not a sprawling list).
CLIENT_KEY_SPECS: Dict[str, Dict[str, Any]] = {
    "codex": {
        "id": "codex",
        "label": "Codex (OpenAI)",
        "env_var": "OPENAI_API_KEY",
    },
    "gemini": {
        "id": "gemini",
        "label": "Gemini (Google)",
        "env_var": "GEMINI_API_KEY",
    },
}


def known_client_key_ids() -> List[str]:
    return sorted(CLIENT_KEY_SPECS.keys())


def provider_for(provider_id: Optional[str]) -> Dict[str, Any]:
    """Metadata dict for a provider id; unknown/absent → anthropic (so a
    stale member file or a provider from a future daemon degrades safely
    to native Anthropic instead of crashing a spawn)."""
    if not provider_id:
        return PROVIDERS[DEFAULT_PROVIDER]
    return PROVIDERS.get(str(provider_id).strip().lower(), PROVIDERS[DEFAULT_PROVIDER])


def provider_models(provider_id: Optional[str]) -> List[Dict[str, Any]]:
    return list(provider_for(provider_id).get("models") or [])


def is_default_provider(provider_id: Optional[str]) -> bool:
    return (str(provider_id or DEFAULT_PROVIDER).strip().lower()) == DEFAULT_PROVIDER


def build_launch_env(
    base_env: Dict[str, str],
    provider_id: Optional[str],
    *,
    resolved: Optional[Dict[str, Any]] = None,
) -> Dict[str, str]:
    """Return a NEW env dict for `subprocess.Popen(env=…)`.

    The contract (security-critical, tested in tests/test_providers.py):

      1. ALWAYS start from a COPY of `base_env` and REMOVE the
         cross-provider keys (`ANTHROPIC_BASE_URL`, `ANTHROPIC_AUTH_TOKEN`)
         so a value from the daemon's own shell can never leak a session
         onto the wrong endpoint — in EITHER direction.
      2. `anthropic` (default / unknown): return the scrubbed env AS-IS
         (native login/config). Also drop `ANTHROPIC_MODEL` /
         `ANTHROPIC_SMALL_FAST_MODEL` so a stray override from a
         previously-ZAI-configured shell doesn't bleed in.
      3. non-anthropic: overlay `ANTHROPIC_BASE_URL` / `ANTHROPIC_AUTH_TOKEN`
         / `ANTHROPIC_MODEL` / `ANTHROPIC_SMALL_FAST_MODEL` from `resolved`
         ({base_url, auth_token, model, small_fast_model}); also drop
         `ANTHROPIC_API_KEY` so only the provider's token authenticates.

    Never mutates `base_env`. `resolved` is provided by the caller
    (providersvc.resolve_provider, augmented with the per-turn model).
    """
    env = dict(base_env)
    for k in _CROSS_KEYS:
        env.pop(k, None)

    pid = str(provider_id or DEFAULT_PROVIDER).strip().lower()
    if pid not in PROVIDERS or pid == DEFAULT_PROVIDER:
        # Native Anthropic — a clean slate. Drop model overrides too.
        env.pop("ANTHROPIC_MODEL", None)
        env.pop("ANTHROPIC_SMALL_FAST_MODEL", None)
        return env

    resolved = resolved or {}
    base_url = str(resolved.get("base_url") or "").strip()
    token = str(resolved.get("auth_token") or "").strip()
    model = str(resolved.get("model") or "").strip()
    small = str(resolved.get("small_fast_model") or "").strip()

    if base_url:
        env["ANTHROPIC_BASE_URL"] = base_url
    if token:
        env["ANTHROPIC_AUTH_TOKEN"] = token
    if model:
        env["ANTHROPIC_MODEL"] = model
    if small:
        env["ANTHROPIC_SMALL_FAST_MODEL"] = small
    # A custom-endpoint provider authenticates with AUTH_TOKEN — a
    # lingering ANTHROPIC_API_KEY (used by the daemon's own /team/draft
    # call) would confuse the CLI's credential precedence. Drop it.
    env.pop("ANTHROPIC_API_KEY", None)
    return env
