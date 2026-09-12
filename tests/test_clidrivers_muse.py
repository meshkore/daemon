"""DM-CLI-11: real Muse 1.1.1 envelopes, login gating and config wiring."""

import io
import json
import sys
from pathlib import Path
from types import SimpleNamespace

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from clidrivers import DRIVERS, driver_for  # noqa: E402
from clidrivers.base import Final, TextDelta, ToolResult, ToolUse  # noqa: E402
from clidrivers.muse import MuseDriver  # noqa: E402
from clidriverssvc import ClientsMixin  # noqa: E402
from runnerspawn import RunnerSpawnMixin  # noqa: E402


def test_real_tool_transcript():
    driver = MuseDriver()
    lines = (
        Path(__file__)
        .with_name("fixtures")
        .joinpath("muse-1.1.1-tool.jsonl")
        .read_text()
        .splitlines()
    )
    events = [event for line in lines for event in driver.parse_stream_line(line)]
    assert (
        "".join(e.text for e in events if isinstance(e, TextDelta)) == "MUSE_TOOL_READY"
    )
    assert [e.text for e in events if isinstance(e, Final)] == ["MUSE_TOOL_READY"]
    assert [e.name for e in events if isinstance(e, ToolUse)] == ["bash"]
    assert [e.ok for e in events if isinstance(e, ToolResult)] == [True]


@pytest.mark.parametrize(
    "line",
    [
        "banner",
        "null",
        "[]",
        "1",
        "{}",
        '{"payload":[]}',
        '{"payload_type":"run.output.delta","payload":{"text":123}}',
    ],
)
def test_malformed_lines_are_ignored(line):
    assert MuseDriver().parse_stream_line(line) == []


@pytest.mark.parametrize("kind", ["failed", "cancelled"])
def test_terminal_failure_is_visible(kind):
    event = {
        "payload_type": f"run.terminal.{kind}",
        "payload": {"reason": "authentication required"},
    }
    result = MuseDriver().parse_stream_line(json.dumps(event))
    assert isinstance(result[0], Final)
    assert "authentication required" in result[0].text


def test_subtask_failures_do_not_finalize_parent():
    event = {
        "payload_type": "task.lifecycle.failed",
        "payload": {"event": {"reason": "child failed"}},
    }
    assert MuseDriver().parse_stream_line(json.dumps(event)) == []


def test_arguments_and_stdin():
    driver = MuseDriver()
    prompt = "Read the task\nPreserve $HOME and `literal` arguments"
    args = driver.build_args(
        "/bin/muse",
        prompt=prompt,
        model="muse-spark-1.3",
        effort="ultra",
        session_id="unused",
        use_session=True,
    )
    assert args[:3] == ["/bin/muse", "exec", "--json"]
    assert prompt not in args
    assert args[-2:] == ["--prompt-file", "/dev/stdin"]
    assert driver.prompt_file_stdin is True
    assert args[args.index("--model") + 1] == "muse-spark-1.3"
    assert args[args.index("--reasoning-effort") + 1] == "ultra"
    assert "--session-id" not in args
    assert "--yolo" in args
    proc = SimpleNamespace(stdin=io.BytesIO())
    driver.write_prompt(proc, prompt)
    assert proc.stdin.closed


def test_default_model_and_effort_are_not_forced():
    args = MuseDriver().build_args(
        "muse",
        prompt="hi",
        model="",
        effort="default",
        session_id="",
        use_session=False,
    )
    assert "--model" not in args and "--reasoning-effort" not in args
    assert driver_for("muse").id == "muse"


@pytest.fixture
def auth_path(tmp_path, monkeypatch):
    monkeypatch.delenv("META_API_KEY", raising=False)
    monkeypatch.delenv("MUSE_AUTH_PATH", raising=False)
    monkeypatch.setenv("XDG_CONFIG_HOME", str(tmp_path))
    path = tmp_path / "muse/auth.json"
    path.parent.mkdir()
    return path


def test_no_login_or_corrupt_file_is_unavailable(auth_path):
    driver = MuseDriver()
    assert driver.auth_configured() is False
    for text in [
        "not-json",
        "null",
        "[]",
        '{"providers":null}',
        '{"providers":{"meta":[]}}',
        "{}",
    ]:
        auth_path.write_text(text)
        assert driver.auth_configured() is False


@pytest.mark.parametrize(
    "meta",
    [
        {"mechanism": "oauth", "storage": "keychain"},
        {"mechanism": "api_key", "storage": "keychain"},
        {"mechanism": "oauth", "access_token": "test-only"},
        {"mechanism": "api_key", "api_key": "test-only"},
    ],
)
def test_native_credential_forms(auth_path, meta):
    auth_path.write_text(json.dumps({"providers": {"meta": meta}}))
    assert MuseDriver().auth_configured() is True


def test_env_and_auth_path_override(auth_path, monkeypatch, tmp_path):
    monkeypatch.setenv("META_API_KEY", "test-only")
    assert MuseDriver().auth_configured() is True
    monkeypatch.delenv("META_API_KEY")
    alternative = tmp_path / "alternate.json"
    alternative.write_text(
        '{"providers":{"meta":{"mechanism":"oauth","storage":"keychain"}}}'
    )
    monkeypatch.setenv("MUSE_AUTH_PATH", str(alternative))
    assert MuseDriver().auth_configured() is True


def test_binary_fallback(monkeypatch, tmp_path):
    monkeypatch.setenv("HOME", str(tmp_path))
    monkeypatch.setattr("clidrivers.muse.shutil.which", lambda name: None)
    driver = MuseDriver()
    assert driver.find_binary() is None
    binary = tmp_path / ".local/bin/muse"
    binary.parent.mkdir(parents=True)
    binary.write_text("#!/bin/sh\n")
    binary.chmod(0o600)
    assert driver.find_binary() is None
    binary.chmod(0o700)
    assert driver.find_binary() == str(binary)


def test_listing_key_login_and_disabled(monkeypatch):
    driver = DRIVERS["muse"]
    monkeypatch.setattr(driver, "find_binary", lambda: "/bin/muse")
    monkeypatch.setattr(driver, "auth_configured", lambda: False)

    class Host(ClientsMixin):
        key = None
        enabled = True

        def resolve_client_key(self, client_id):
            return self.key if client_id == "muse" else None

        def client_enabled(self, client_id):
            return self.enabled

    host = Host()

    def entry():
        return next(c for c in host.clients_listing() if c["id"] == "muse")

    assert entry()["authConfigured"] is False
    host.key = "test-only-secret"
    assert entry()["authConfigured"] is True
    assert "test-only-secret" not in json.dumps(entry())
    host.enabled = False
    assert entry()["authConfigured"] is False


@pytest.mark.parametrize("enabled,authed", [(True, False), (False, True)])
def test_runner_blocks_unavailable_muse_before_popen(monkeypatch, enabled, authed):
    driver = DRIVERS["muse"]
    monkeypatch.setattr(driver, "find_binary", lambda: "/bin/muse")
    monkeypatch.setattr(driver, "auth_configured", lambda: authed)
    monkeypatch.delenv("META_API_KEY", raising=False)

    class Host:
        def resolve_client_key(self, client_id):
            return None

        def client_enabled(self, client_id):
            return enabled

    class Runner(RunnerSpawnMixin):
        client = "muse"
        conv = "test-muse"
        model = ""
        effort = "default"
        daemon = Host()
        error = None

        def _briefing(self):
            return "test"

        def _emit_runner_error(self, text):
            self.error = text

    monkeypatch.setattr(
        "subprocess.Popen", lambda *a, **k: pytest.fail("must not spawn")
    )
    runner = Runner()
    runner.spawn()
    assert "muse login" in runner.error


@pytest.mark.parametrize("effort", ["none", "minimal", "ultra", "max"])
def test_conversation_preserves_muse_effort(effort):
    from convmeta import ConvMetaMixin

    host = SimpleNamespace(
        _conv_meta_load=lambda: {"muse-test": {"client": "muse", "effort": effort}}
    )
    assert ConvMetaMixin._conv_meta_get_effort(host, "muse-test") == effort


def test_conversation_rejects_effort_outside_client_catalog():
    from convmeta import ConvMetaMixin

    host = SimpleNamespace(
        _conv_meta_load=lambda: {"test": {"client": "gemini", "effort": "ultra"}}
    )
    assert ConvMetaMixin._conv_meta_get_effort(host, "test") is None
