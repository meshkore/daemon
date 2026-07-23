"""test_providers.py — multi-provider-agents (MPV1).

Covers the three load-bearing pieces of provider dispatch:

  1. `providers.build_launch_env` — the SECURITY-critical env builder. An
     Anthropic turn must NEVER inherit a stray ZAI base-url/token from the
     daemon's own shell, and a ZAI turn must get the overlay + drop the
     Anthropic key. Also: never mutate the caller's base_env.
  2. `providersvc` — the machine-global config + chmod-0600 key store +
     `resolve_provider` availability logic + the `/config/providers`
     get/set surface (keys never returned).
  3. `team.py` — the new `provider` schema field (default anthropic,
     validated, round-trips through serialise/split).
"""

from __future__ import annotations

import io
import os
import stat
import subprocess
import sys
from pathlib import Path
from typing import Any

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

import daemon as d  # type: ignore[import-not-found]  # noqa: E402
from clidrivers import DRIVERS  # noqa: E402
from globalledger import GlobalLedger  # noqa: E402
from providers import (  # noqa: E402
    DEFAULT_PROVIDER,
    build_launch_env,
    known_provider_ids,
    provider_for,
    provider_models,
)
from providersvc import ProviderKeyStore, ProvidersMixin  # noqa: E402
from team import (  # noqa: E402
    TeamValidationError,
    _normalise_payload,
    serialise_member,
    split_member_file,
    validate_member,
)


# ── registry ─────────────────────────────────────────────────────────────


def test_registry_has_anthropic_default_and_zai() -> None:
    ids = known_provider_ids()
    assert "anthropic" in ids and "zai" in ids
    assert DEFAULT_PROVIDER == "anthropic"
    # unknown / None degrade to anthropic (never crash a spawn)
    assert provider_for("nope")["id"] == "anthropic"
    assert provider_for(None)["id"] == "anthropic"
    assert any(m["id"] == "glm-4.6" for m in provider_models("zai"))


# ── build_launch_env (security-critical) ──────────────────────────────────


def _polluted_base() -> dict:
    # Simulate a daemon shell that already has stray Anthropic-family vars
    # (e.g. an operator who exported a ZAI config into their own env).
    return {
        "PATH": "/usr/bin",
        "ANTHROPIC_BASE_URL": "https://leaked.example/anthropic",
        "ANTHROPIC_AUTH_TOKEN": "leaked-token",
        "ANTHROPIC_API_KEY": "sk-ant-native",
        "ANTHROPIC_MODEL": "leaked-model",
        "ANTHROPIC_SMALL_FAST_MODEL": "leaked-small",
    }


def test_anthropic_scrubs_cross_keys_but_keeps_api_key() -> None:
    base = _polluted_base()
    env = build_launch_env(base, "anthropic", resolved={"model": "opus"})
    # cross-provider endpoint/token must be gone — no leak onto a custom URL
    assert "ANTHROPIC_BASE_URL" not in env
    assert "ANTHROPIC_AUTH_TOKEN" not in env
    # stray model overrides dropped so native config decides
    assert "ANTHROPIC_MODEL" not in env
    assert "ANTHROPIC_SMALL_FAST_MODEL" not in env
    # native key login preserved (operator may rely on it)
    assert env["ANTHROPIC_API_KEY"] == "sk-ant-native"
    assert env["PATH"] == "/usr/bin"
    # base_env untouched (no mutation)
    assert base["ANTHROPIC_BASE_URL"] == "https://leaked.example/anthropic"


def test_zai_overlays_and_drops_native_api_key() -> None:
    base = _polluted_base()
    resolved = {
        "base_url": "https://api.z.ai/api/anthropic",
        "auth_token": "zai-secret",
        "small_fast_model": "glm-4.5-air",
        "model": "glm-4.6",
        "available": True,
    }
    env = build_launch_env(base, "zai", resolved=resolved)
    assert env["ANTHROPIC_BASE_URL"] == "https://api.z.ai/api/anthropic"
    assert env["ANTHROPIC_AUTH_TOKEN"] == "zai-secret"
    assert env["ANTHROPIC_MODEL"] == "glm-4.6"
    assert env["ANTHROPIC_SMALL_FAST_MODEL"] == "glm-4.5-air"
    # a custom-endpoint provider authenticates with AUTH_TOKEN — the native
    # ANTHROPIC_API_KEY must not linger and confuse credential precedence
    assert "ANTHROPIC_API_KEY" not in env


def test_unknown_provider_behaves_like_anthropic() -> None:
    env = build_launch_env(_polluted_base(), "ghost", resolved={})
    assert "ANTHROPIC_BASE_URL" not in env
    assert "ANTHROPIC_AUTH_TOKEN" not in env
    assert env.get("ANTHROPIC_API_KEY") == "sk-ant-native"


def test_zai_without_resolved_values_still_scrubs() -> None:
    # If config resolution failed, we must NOT keep the leaked cross keys.
    env = build_launch_env(_polluted_base(), "zai", resolved={})
    assert "ANTHROPIC_BASE_URL" not in env
    assert "ANTHROPIC_AUTH_TOKEN" not in env


# ── providersvc: config + key store + resolve ──────────────────────────────


class _FakeDaemon(ProvidersMixin):
    def __init__(self, ledger: GlobalLedger) -> None:
        self.global_ledger = ledger


def _daemon(tmp_path: Path) -> _FakeDaemon:
    return _FakeDaemon(GlobalLedger(root=tmp_path / "ledger"))


def test_resolve_zai_unavailable_until_key_set(tmp_path: Path) -> None:
    d = _daemon(tmp_path)
    r = d.resolve_provider("zai")
    assert r["requires_key"] is True
    assert r["available"] is False  # no key yet
    assert r["auth_token"] is None
    # anthropic never needs a key
    assert d.resolve_provider("anthropic")["available"] is True

    # set the key via the HTTP surface
    code, _ = d.provider_config_set_http({"providers": {"zai": {"key": "zai-xyz"}}})
    assert code == 200
    r2 = d.resolve_provider("zai")
    assert r2["available"] is True
    assert r2["auth_token"] == "zai-xyz"
    assert r2["base_url"]  # default seeded


def test_key_file_is_0600(tmp_path: Path) -> None:
    d = _daemon(tmp_path)
    d.provider_config_set_http({"providers": {"zai": {"key": "secret"}}})
    store = ProviderKeyStore(d.global_ledger)
    p = store._path("zai")
    assert p.is_file()
    mode = stat.S_IMODE(os.stat(p).st_mode)
    assert mode == 0o600, oct(mode)


def test_config_get_never_returns_key_value(tmp_path: Path) -> None:
    d = _daemon(tmp_path)
    d.provider_config_set_http(
        {"providers": {"zai": {"key": "super-secret", "base_url": "https://x/y"}}}
    )
    code, body = d.provider_config_get_http()
    assert code == 200
    zai = next(p for p in body["providers"] if p["id"] == "zai")
    assert zai["keyPresent"] is True
    assert zai["baseUrl"] == "https://x/y"
    assert "key" not in zai and "auth_token" not in zai
    # the raw secret appears nowhere in the serialized response
    import json

    assert "super-secret" not in json.dumps(body)


def test_clear_key_disables_availability(tmp_path: Path) -> None:
    d = _daemon(tmp_path)
    d.provider_config_set_http({"providers": {"zai": {"key": "k"}}})
    assert d.resolve_provider("zai")["available"] is True
    d.provider_config_set_http({"providers": {"zai": {"clear_key": True}}})
    assert d.resolve_provider("zai")["available"] is False


def test_disabled_provider_not_available_even_with_key(tmp_path: Path) -> None:
    d = _daemon(tmp_path)
    d.provider_config_set_http({"providers": {"zai": {"key": "k", "enabled": False}}})
    assert d.resolve_provider("zai")["available"] is False


def test_public_listing_carries_no_secret(tmp_path: Path) -> None:
    d = _daemon(tmp_path)
    d.provider_config_set_http({"providers": {"zai": {"key": "topsecret"}}})
    import json

    listing = d.providers_public_listing()
    assert "topsecret" not in json.dumps(listing)
    zai = next(p for p in listing if p["id"] == "zai")
    assert zai["available"] is True and zai["requiresKey"] is True


def test_keystore_key_survives_config_rewrite(tmp_path: Path) -> None:
    # Setting an UNRELATED field must not wipe the key (keys live outside
    # clients-config.json).
    d = _daemon(tmp_path)
    d.provider_config_set_http({"providers": {"zai": {"key": "k"}}})
    d.provider_config_set_http({"providers": {"gemini": {"enabled": False}}})
    assert ProviderKeyStore(d.global_ledger).present("zai") is True


# ── unified list: Codex/Gemini client keys alongside Anthropic/ZAI ────────


def test_unified_listing_includes_codex_and_gemini(tmp_path: Path) -> None:
    d = _daemon(tmp_path)
    code, body = d.provider_config_get_http()
    assert code == 200
    ids = {p["id"] for p in body["providers"]}
    assert ids == {"anthropic", "zai", "codex", "gemini"}
    codex = next(p for p in body["providers"] if p["id"] == "codex")
    gemini = next(p for p in body["providers"] if p["id"] == "gemini")
    # Codex/Gemini have no base-url swap — the cockpit shouldn't render
    # those inputs for them (only ZAI-like claude-code providers do).
    assert codex["hasEndpoint"] is False and gemini["hasEndpoint"] is False
    assert codex["requiresKey"] is True and gemini["requiresKey"] is True
    zai = next(p for p in body["providers"] if p["id"] == "zai")
    assert zai["hasEndpoint"] is True


def test_codex_available_without_key_native_login_still_works(tmp_path: Path) -> None:
    # Unlike ZAI, Codex/Gemini keep working via their own native login even
    # with no daemon-stored key — the key is an optional convenience.
    d = _daemon(tmp_path)
    code, body = d.provider_config_get_http()
    codex = next(p for p in body["providers"] if p["id"] == "codex")
    assert codex["keyPresent"] is False
    assert codex["available"] is True


def test_resolve_client_key_absent_then_set(tmp_path: Path) -> None:
    d = _daemon(tmp_path)
    assert d.resolve_client_key("codex") is None
    d.provider_config_set_http({"providers": {"codex": {"key": "sk-codex-xyz"}}})
    assert d.resolve_client_key("codex") == "sk-codex-xyz"
    # anthropic/zai (claude-code providers) are NOT client keys
    assert d.resolve_client_key("anthropic") is None
    assert d.resolve_client_key("zai") is None


def test_client_key_file_is_also_0600(tmp_path: Path) -> None:
    d = _daemon(tmp_path)
    d.provider_config_set_http({"providers": {"gemini": {"key": "gem-secret"}}})
    p = ProviderKeyStore(d.global_ledger)._path("gemini")
    assert p.is_file()
    assert stat.S_IMODE(os.stat(p).st_mode) == 0o600


# ── team schema: the `provider` field ──────────────────────────────────────


def test_normalise_defaults_provider_to_anthropic() -> None:
    fm = _normalise_payload({"id": "zx", "kind": "profile"}, today="2026-07-09")
    assert fm["provider"] == "anthropic"


def test_normalise_accepts_zai_provider() -> None:
    fm = _normalise_payload(
        {"id": "zx", "kind": "profile", "provider": "ZAI", "model": "glm-4.6"},
        today="2026-07-09",
    )
    assert fm["provider"] == "zai"
    validate_member(fm)  # must not raise


def test_validate_rejects_unknown_provider() -> None:
    fm = _normalise_payload({"id": "zx", "kind": "profile"}, today="2026-07-09")
    fm["provider"] = "openai"
    with pytest.raises(TeamValidationError):
        validate_member(fm)


def test_provider_roundtrips_through_serialise() -> None:
    fm = _normalise_payload(
        {"id": "zx", "kind": "profile", "provider": "zai", "model": "glm-4.6"},
        today="2026-07-09",
    )
    text = serialise_member(fm, "body\n")
    parsed, _ = split_member_file(text)
    assert parsed["provider"] == "zai"


def test_absent_provider_validates_as_anthropic() -> None:
    # A pre-MPV1 member file has no `provider` key at all — must still pass.
    fm = _normalise_payload({"id": "zx", "kind": "profile"}, today="2026-07-09")
    del fm["provider"]
    validate_member(fm)  # no raise; treated as anthropic


# ── spawn-level env injection for Codex/Gemini client keys ────────────────
#
# Reuses the fake-Popen harness from test_refactor_characterization.py
# (same shape) but ALSO captures the `env` kwarg, which that harness never
# needed to inspect.


class _FakeHub:
    def broadcast(self, *a: Any, **k: Any) -> None:
        pass


class _EnvCapturingProc:
    last_env: dict = {}

    def __init__(self, args: list, **kw: Any) -> None:
        type(self).last_env = dict(kw.get("env") or {})
        self.pid = 4242
        self.stdin = io.BytesIO()
        self.stdout = io.BytesIO(b"")
        self.stderr = io.BytesIO(b"")

    def poll(self) -> int:
        return 0

    def wait(self, timeout: float | None = None) -> int:
        return 0

    def kill(self) -> None:
        pass


class _FakeChatArchive:
    def is_archived(self, conv: str) -> bool:
        # Pretend already archived so the reader thread's finalize path
        # (runnerloop.py) short-circuits its auto-archive branch cleanly —
        # this test only cares about the env spawn() builds, not finalize.
        return True


class _FakeDaemonWithClientKey:
    def __init__(self, keys: dict) -> None:
        self._keys = keys
        self.chat_archive = _FakeChatArchive()

    def resolve_client_key(self, client_id: str):
        return self._keys.get(client_id)


def _spawn_capture_env(
    cluster_root: Path, monkeypatch: Any, *, client: str, daemon_obj: Any
) -> dict:
    monkeypatch.setattr(subprocess, "Popen", _EnvCapturingProc)
    monkeypatch.setattr(DRIVERS[client], "find_binary", lambda: f"/usr/bin/{client}")
    paths = d.Paths(cluster_root)
    clu = d.Cluster(paths)
    runner = d.ChatRunner(
        paths=paths,
        cluster=clu,
        hub=_FakeHub(),
        identity="id-x",
        conv="conv-x",
        prompt="hi",
        client=client,
        daemon=daemon_obj,
    )
    runner.spawn()
    runner.done.wait(timeout=5)
    return _EnvCapturingProc.last_env


def test_codex_spawn_injects_stored_key(cluster, monkeypatch: Any) -> None:
    root = cluster("populated")
    daemon_obj = _FakeDaemonWithClientKey({"codex": "sk-codex-injected"})
    env = _spawn_capture_env(root, monkeypatch, client="codex", daemon_obj=daemon_obj)
    assert env.get("OPENAI_API_KEY") == "sk-codex-injected"


def test_gemini_spawn_injects_stored_key(cluster, monkeypatch: Any) -> None:
    root = cluster("populated")
    daemon_obj = _FakeDaemonWithClientKey({"gemini": "gem-injected"})
    env = _spawn_capture_env(root, monkeypatch, client="gemini", daemon_obj=daemon_obj)
    assert env.get("GEMINI_API_KEY") == "gem-injected"


def test_codex_spawn_leaves_env_untouched_when_no_stored_key(
    cluster, monkeypatch: Any
) -> None:
    # No daemon-managed key → native env/login is left exactly as inherited
    # (no OPENAI_API_KEY injected, none removed).
    root = cluster("populated")
    daemon_obj = _FakeDaemonWithClientKey({})
    monkeypatch.setenv("OPENAI_API_KEY", "operators-own-shell-key")
    env = _spawn_capture_env(root, monkeypatch, client="codex", daemon_obj=daemon_obj)
    assert env.get("OPENAI_API_KEY") == "operators-own-shell-key"
