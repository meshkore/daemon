"""Meta Muse Code driver (DM-CLI-11).

Verified against Muse Code 1.1.1 on 2026-09-12: exec --json emits
run.output.delta and run.terminal.* envelopes, not MSP notifications.
Models come from the installed CLI's MSP model/list catalog. The default
currently resolves to muse-spark-1.3-contributor; contributor labels retain
Meta's content-use distinction. Credentials belong to Muse, never MeshKore.
The runner supplies a regular temporary file as stdin: --prompt-file
/dev/stdin accepts it, avoids argv size limits and leaves no named prompt
file. A pipe is rejected by Muse. Cross-turn history remains daemon-owned.
"""

from __future__ import annotations

import json
import os
import shutil
from pathlib import Path
from typing import Any, Dict, List, Optional

from .base import ClientDriver, Final, TextDelta, ToolResult, ToolUse


class MuseDriver(ClientDriver):
    prompt_file_stdin = True
    id = "muse"
    label = "Muse Code (Meta)"

    def find_binary(self) -> Optional[str]:
        found = shutil.which("muse")
        if found:
            return found
        candidate = os.path.expanduser("~/.local/bin/muse")
        return (
            candidate
            if os.path.isfile(candidate) and os.access(candidate, os.X_OK)
            else None
        )

    def install_hint(self) -> str:
        return "curl -fsSL https://dev.meta.ai/install.sh | bash; then muse login"

    def auth_configured(self) -> Optional[bool]:
        if os.environ.get("META_API_KEY", "").strip():
            return True
        config = Path(os.environ.get("XDG_CONFIG_HOME") or Path.home() / ".config")
        path = Path(os.environ.get("MUSE_AUTH_PATH") or config / "muse/auth.json")
        try:
            record = json.loads(path.read_text())
            meta = record.get("providers", {}).get("meta", {})
            if not isinstance(meta, dict):
                return False
            # Muse's successful macOS device login stores tokens in Keychain;
            # auth.json contains only this marker. Never read/export tokens
            # from Keychain. As with other drivers, this is a local setup
            # probe, not a network guarantee that the credential is unexpired.
            if (
                meta.get("mechanism") in ("oauth", "api_key")
                and meta.get("storage") == "keychain"
            ):
                return True
            if meta.get("mechanism") == "oauth":
                return meta.get("storage") == "keychain" or bool(
                    meta.get("access_token")
                )
            return bool(meta.get("api_key"))
        except (OSError, ValueError, AttributeError, TypeError):
            return False

    def models_catalog(self) -> List[Dict[str, Any]]:
        return [
            {"id": "", "label": "Default (Muse CLI config)"},
            {"id": "muse-spark-1.3", "label": "Muse Spark 1.3"},
            {
                "id": "muse-spark-1.3-contributor",
                "label": "Muse Spark 1.3 Contributor (content may improve Meta products)",
            },
            {"id": "muse-spark-1.2", "label": "Muse Spark 1.2"},
            {
                "id": "muse-spark-1.2-contributor",
                "label": "Muse Spark 1.2 Contributor (content may improve Meta products)",
            },
        ]

    def efforts_catalog(self) -> List[Dict[str, Any]]:
        return [{"id": "default", "label": "Default (high)"}] + [
            {"id": level, "label": level.title()}
            for level in (
                "none",
                "minimal",
                "low",
                "medium",
                "high",
                "xhigh",
                "max",
                "ultra",
            )
        ]

    def build_args(
        self,
        binary: str,
        *,
        prompt: str,
        model: Optional[str],
        effort: Optional[str],
        session_id: str,
        use_session: bool,
    ) -> List[str]:
        args = [binary, "exec", "--json", "--yolo", "--no-foreign-personal-context"]
        if model:
            args.extend(["--model", model])
        if effort and effort != "default":
            args.extend(["--reasoning-effort", effort])
        args.extend(["--prompt-file", "/dev/stdin"])
        return args

    def write_prompt(self, proc: Any, prompt: str) -> None:
        if proc.stdin is not None:
            proc.stdin.close()

    def parse_stream_line(self, line: str) -> List[Any]:
        try:
            envelope = json.loads(line)
        except ValueError:
            return []
        if not isinstance(envelope, dict):
            return []
        payload = envelope.get("payload")
        if not isinstance(payload, dict):
            return []
        kind = envelope.get("payload_type")
        if kind == "task.lifecycle.side_effect_intent":
            event = payload.get("event")
            if isinstance(event, dict):
                operation = event.get("operation")
                if isinstance(operation, str) and operation.startswith("tool:"):
                    # exec reports the tool name but not its arguments here.
                    return [ToolUse(name=operation[5:], input={})]
        if kind == "tool.result":
            facts = payload.get("correlation_facts")
            if isinstance(facts, dict):
                return [ToolResult(ok=facts.get("outcome") == "success")]
        if kind == "run.output.delta":
            text = payload.get("text")
            return [TextDelta(text)] if isinstance(text, str) and text else []
        if kind in (
            "run.terminal.completed",
            "run.terminal.failed",
            "run.terminal.cancelled",
        ):
            text = payload.get("text")
            if kind != "run.terminal.completed":
                reason = payload.get("reason") or text or "Turn did not complete"
                return [Final(text=f"[muse error] {reason}")]
            return [Final(text=text if isinstance(text, str) else "")]
        return []
