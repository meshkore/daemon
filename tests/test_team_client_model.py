"""test_team_client_model.py — DM-CLI-05 follow-up (multi-cli-clients).

A live smoke test (2026-07-08) found that creating a member with
`client: codex` and no explicit model silently defaulted to `opus` (the
claude-code strongest-alias sentinel) via `_normalise_payload`, which
Codex then rejected outright (`-m opus` → 400 "not supported"). Root
cause: the default was unconditional, not client-aware. These tests
pin the fix: a client whose OWN catalog declares `""` as a legitimate
"use default" entry (codex, gemini) gets an empty model when omitted;
claude-code (no such catalog entry) keeps defaulting to `opus`
unchanged — this is a genuinely additive fix, not a loosened rule.
"""

from __future__ import annotations

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from team import (
    STRONGEST_MODEL_ALIAS,
    TeamValidationError,
    _normalise_payload,
    validate_member,
)  # noqa: E402


def test_normalise_payload_claude_code_defaults_to_strongest_alias() -> None:
    fm = _normalise_payload({"id": "x", "kind": "profile"}, today="2026-07-08")
    assert fm["client"] == "claude-code"
    assert fm["model"] == STRONGEST_MODEL_ALIAS


def test_normalise_payload_codex_omitted_model_stays_empty() -> None:
    fm = _normalise_payload(
        {"id": "x", "kind": "profile", "client": "codex"}, today="2026-07-08"
    )
    assert fm["client"] == "codex"
    assert fm["model"] == ""


def test_normalise_payload_gemini_omitted_model_stays_empty() -> None:
    fm = _normalise_payload(
        {"id": "x", "kind": "profile", "client": "gemini"}, today="2026-07-08"
    )
    assert fm["model"] == ""


def test_normalise_payload_explicit_model_always_wins() -> None:
    fm = _normalise_payload(
        {"id": "x", "kind": "profile", "client": "codex", "model": "gpt-5"},
        today="2026-07-08",
    )
    assert fm["model"] == "gpt-5"


def _base_fm(**overrides: object) -> dict:
    fm = {
        "id": "smoke-test",
        "kind": "profile",
        "required": False,
        "client": "claude-code",
        "model": "opus",
        "effort": "default",
    }
    fm.update(overrides)
    return fm


def test_validate_member_claude_code_still_rejects_empty_model() -> None:
    # Unchanged pre-existing behavior — the empty-model exemption is
    # per-driver, not global.
    try:
        validate_member(_base_fm(model=""))
        raise AssertionError("expected TeamValidationError")
    except TeamValidationError as e:
        assert "model is mandatory" in e.message


def test_validate_member_codex_accepts_empty_model() -> None:
    validate_member(_base_fm(client="codex", model=""))  # must not raise


def test_validate_member_gemini_accepts_empty_model() -> None:
    validate_member(_base_fm(client="gemini", model=""))  # must not raise


def test_validate_member_unknown_client_rejected() -> None:
    try:
        validate_member(_base_fm(client="not-a-real-cli"))
        raise AssertionError("expected TeamValidationError")
    except TeamValidationError as e:
        assert "client must be one of" in e.message
