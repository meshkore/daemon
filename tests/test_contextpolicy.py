"""CTX1 — per-platform context-window policy (contextpolicy.py).

Pure unit tests: window sizing, fill ratio, compaction threshold, and the
JSON-safe `describe()` block the daemon attaches to chat.usage events. No
daemon spawn — the policy is side-effect-free by design.
"""

from __future__ import annotations

from contextpolicy import ClaudeCodePolicy, ContextPolicy, policy_for


def _usage(inp: int = 0, out: int = 0, cr: int = 0, cc: int = 0) -> dict:
    return {
        "input_tokens": inp,
        "output_tokens": out,
        "cache_read_input_tokens": cr,
        "cache_creation_input_tokens": cc,
    }


def test_claude_window_sizing() -> None:
    p = ClaudeCodePolicy()
    # auto / None / "" → conservative default window (gauge still works).
    assert p.context_window(None) == 200_000
    assert p.context_window("auto") == 200_000
    assert p.context_window("") == 200_000
    # short aliases + explicit ids.
    assert p.context_window("opus") == 200_000
    assert p.context_window("claude-opus-4-8") == 200_000
    # 1M variant resolves exactly (and beats the non-[1m] prefix).
    assert p.context_window("claude-opus-4-8[1m]") == 1_000_000
    # Claude 5 family. Sonnet 5 is a 200k tier like the Opus/Sonnet 4.x line;
    # Fable 5's 1M window is NATIVE — both the bare id and the [1m] variant are
    # 1M, so the longest-prefix fallback can't drag "claude-fable-5[1m]" to 200k.
    assert p.context_window("claude-sonnet-5") == 200_000
    assert p.context_window("claude-sonnet-5[1m]") == 1_000_000
    assert p.context_window("claude-fable-5") == 1_000_000
    assert p.context_window("claude-fable-5[1m]") == 1_000_000
    # unknown claude id → family default.
    assert p.context_window("claude-something-new") == 200_000


def test_prompt_tokens_excludes_output() -> None:
    # Prompt window fill = what the model READ (input + both caches), never
    # what it wrote (output).
    u = _usage(inp=10_000, out=9_999_999, cr=5_000, cc=1_000)
    assert ContextPolicy.prompt_tokens(u) == 16_000
    assert ContextPolicy.prompt_tokens(None) == 0
    assert ContextPolicy.prompt_tokens({}) == 0


def test_fill_ratio_and_threshold() -> None:
    p = ClaudeCodePolicy()
    # 100k read on a 200k window = 0.5 → at the 50% threshold → compact.
    half = _usage(inp=100_000)
    assert p.fill_ratio(half, "opus") == 0.5
    assert p.should_compact(half, "opus") is True
    # Just under → no compaction.
    low = _usage(inp=80_000)
    assert p.fill_ratio(low, "opus") == 0.4
    assert p.should_compact(low, "opus") is False
    # Over-full clamps to 1.0.
    assert p.fill_ratio(_usage(inp=500_000), "opus") == 1.0


def test_generic_platform_is_inert() -> None:
    # An unmodelled runtime: unknown window → no gauge, never claims to compact.
    g = ContextPolicy()
    assert g.context_window("whatever") is None
    assert g.fill_ratio(_usage(inp=999_999), "whatever") is None
    assert g.should_compact(_usage(inp=999_999), "whatever") is False


def test_policy_registry() -> None:
    assert isinstance(policy_for("claude-code"), ClaudeCodePolicy)
    # Unknown / None platform → generic (NOT claude-code) so we never assume
    # claude behaviour for a runtime we don't model.
    assert policy_for("deepseek-cli").__class__ is ContextPolicy
    assert policy_for(None).__class__ is ContextPolicy


def test_describe_shape() -> None:
    p = ClaudeCodePolicy()
    block = p.describe(_usage(inp=100_000), "opus")
    assert block == {
        "platform": "claude-code",
        "window": 200_000,
        "prompt_tokens": 100_000,
        "fill_ratio": 0.5,
        "supports_compaction": True,
        "threshold": 0.5,
        "should_compact": True,
    }
    # Generic: window/fill None, threshold None, never compacts.
    gblock = ContextPolicy().describe(_usage(inp=100_000), "x")
    assert gblock["window"] is None
    assert gblock["fill_ratio"] is None
    assert gblock["threshold"] is None
    assert gblock["should_compact"] is False
