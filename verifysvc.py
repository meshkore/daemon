"""verifysvc.py — VerifyMixin: the daemon side of MeshKore Verify (VRF2).

Exposes the local Verifier (verify.py / VRF1) to agents over `POST /verify` and
closes the blind-agent loop. The render happens HERE, in the daemon, as a local
tool: it drives a host-discovered headless browser over CDP (stdlib only) and
hands back evidence the calling agent's own vision can read.

Config — `cluster.yaml#verify` (all optional):

    verify:
      mode: local | agent          # local = embedded CDP (default); agent = remote A2A (VRF6)
      agent_url: https://…          # when mode: agent
      browser: chrome | chromium    # informational; discovery auto-picks
      allow_download: true          # download Chrome-for-Testing if no host browser
      default_viewports: [...]      # override the mobile+desktop default
      inline_b64: false             # also return screenshots as base64 (for remote/tablet)

Evidence (PNGs + report.json) lands under `.meshkore/.runtime/verify/<conv>/`
(gitignored runtime). A `verify.result` WS event is broadcast so the cockpit
Verify panel (VRF7) can render it.

The post-task auto-hook (run a task's declared `verify:` block on finish and
inject the shots into the agent's next turn) lands with VRF4's frontmatter
block — `run_for_spec()` here is the reusable core it will call.
"""

from __future__ import annotations

import json
import time
from typing import Any, Dict, Optional, Tuple

from utils import _iso_now, _log

# Sibling module (VRF1). In the single-file bundle this `from verify import …`
# line is stripped and the inlined `verify` function is already a flat global —
# so referencing `verify(...)` below works identically in source and bundle.
from verify import verify


class VerifyMixin:
    def _verify_config(self) -> Dict[str, Any]:
        cfg = self.cluster.data.get("verify")
        return cfg if isinstance(cfg, dict) else {}

    def verify_request(self, body: Dict[str, Any]) -> Tuple[int, Dict[str, Any]]:
        """Handle `POST /verify`. Body is a verify spec (`{url, viewports?,
        steps?, checks?, conv?}`). Runs the Verifier locally (or proxies to a
        remote agent when `verify.mode: agent`) and returns the evidence."""
        if not isinstance(body, dict) or not body.get("url"):
            return 400, {"error": "spec.url is required"}
        cfg = self._verify_config()

        if cfg.get("mode") == "agent":
            return self._verify_via_agent(cfg, body)

        # ── local mode: embed VRF1 ──────────────────────────────────────
        conv = str(body.get("conv") or "adhoc")
        safe_conv = "".join(c for c in conv if c.isalnum() or c in "-_") or "adhoc"
        out_dir = self.paths.runtime / "verify" / safe_conv / str(int(time.time()))

        spec = dict(body)
        spec.setdefault("allow_download", cfg.get("allow_download", True))
        if "viewports" not in spec and cfg.get("default_viewports"):
            spec["viewports"] = cfg["default_viewports"]
        if cfg.get("inline_b64"):
            spec.setdefault("inline_b64", True)
        # Keep a downloaded Chromium INSIDE the cluster, with the daemon — not
        # in the per-user OS cache. ~150 MB, gitignored runtime, reusable.
        spec.setdefault("browser_dir", str(self.paths.runtime / "browser"))

        try:
            result = verify(spec, out_dir=str(out_dir), log=_log)
        except Exception as e:  # noqa: BLE001
            _log(f"verify: run failed: {e}")
            return 500, {"error": f"verify failed: {e}"}

        # serve shots back over loopback so a local agent can fetch them
        for shot in result.get("shots", []):
            p = shot.get("path", "")
            if p:
                shot["loopback"] = f"/verify/shot?path={p}"

        self._broadcast_verify(conv, result)
        return 200, result

    def run_for_spec(
        self, spec: Dict[str, Any], conv: str = "adhoc"
    ) -> Optional[Dict[str, Any]]:
        """Reusable core for the post-task hook (VRF4): run a verify spec and
        return the evidence, swallowing errors into a fail verdict so a hook
        never crashes a turn."""
        status, payload = self.verify_request({**spec, "conv": conv})
        if status != 200:
            return {"verdict": "fail", "fail_reasons": [payload.get("error", "error")]}
        return payload

    # ── remote A2A mode (VRF6) ──────────────────────────────────────────
    def _verify_via_agent(
        self, cfg: Dict[str, Any], body: Dict[str, Any]
    ) -> Tuple[int, Dict[str, Any]]:
        agent_url = cfg.get("agent_url")
        if not agent_url:
            return 400, {"error": "verify.mode=agent but verify.agent_url unset"}
        import urllib.request

        try:
            req = urllib.request.Request(
                agent_url.rstrip("/") + "/verify",
                data=json.dumps(body).encode(),
                headers={"content-type": "application/json"},
                method="POST",
            )
            with urllib.request.urlopen(req, timeout=120) as r:
                result = json.loads(r.read())
        except Exception as e:  # noqa: BLE001
            return 502, {"error": f"remote verify agent failed: {e}"}
        self._broadcast_verify(str(body.get("conv") or "adhoc"), result)
        return 200, result

    # ── broadcast ───────────────────────────────────────────────────────
    def _broadcast_verify(self, conv: str, result: Dict[str, Any]) -> None:
        try:
            self.hub.broadcast(
                {
                    "type": "verify.result",
                    "conv": conv,
                    "verdict": result.get("verdict"),
                    "fail_reasons": result.get("fail_reasons", []),
                    "shots": [
                        {"viewport": s.get("viewport"), "loopback": s.get("loopback")}
                        for s in result.get("shots", [])
                    ],
                    "ts": _iso_now(),
                }
            )
        except Exception:
            pass
