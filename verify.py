"""verify.py — MeshKore Verify: the daemon-local visual+functional verifier.

Initiative `meshkore-verify`, tasks VRF1 (the verifier core) + VRF2 (daemon
wiring). The blind-agent loop closer: an agent that edits a UI must stand it up
against a URL, drive it, and LOOK at the evidence before declaring done.

Design (the operator's "ideal, local-first" call, 2026-06-20)
-------------------------------------------------------------
The render is *just a local tool the daemon runs* — no extra service, no
install. We drive a headless Chromium-family browser the daemon DISCOVERS on
the host (Chrome/Brave/Edge on macOS; Edge ships preinstalled on Windows →
zero download there) over the Chrome DevTools Protocol, using ONLY the Python
standard library — `socket`/`struct`/`ssl`/`json`/`subprocess`/`urllib`, all
already daemon dependencies. No Playwright, no pip, no `playwright install`:
the daemon stays a single zero-install `.py`.

If no browser is found we download Chrome-for-Testing (a plain .zip, no
installer, no admin, no PATH change) into the OS cache, OUTSIDE the project.

The whole loop is local: agent edits → `POST /verify` runs the browser on the
host → PNGs land under `.meshkore/.runtime/verify/` → the local agent session
reads the images with its own vision → fix → re-verify until `verdict: pass`.
The SAME contract can later proxy to a remote A2A Verify agent (VRF6) for
tablets/Cloud — a config flip, not a different codebase.

This module is import-clean (stdlib only) and runnable standalone as a CLI
(`python verify.py <url> ...`), so VRF6 can wrap it as an agent unchanged.
"""

from __future__ import annotations

import base64
import json
import os
import platform
import shutil
import socket
import ssl
import struct
import subprocess
import sys
import tempfile
import threading
import time
import urllib.error
import urllib.request
import zipfile
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

# ─────────────────────────────────────────────────────────────────────────────
# 0. small helpers
# ─────────────────────────────────────────────────────────────────────────────


def _now_ms() -> int:
    return int(time.time() * 1000)


def _cache_root(browser_dir: Optional[Path] = None) -> Path:
    """Where a downloaded browser is unpacked. The daemon passes an explicit
    dir INSIDE the cluster (`.meshkore/.runtime/browser`) so the binary lives
    with the project, not the OS cache — `$MESHKORE_BROWSER_DIR` overrides, and
    only the standalone CLI (no dir given) falls back to the per-user OS cache."""
    if browser_dir is not None:
        return Path(browser_dir)
    env = os.environ.get("MESHKORE_BROWSER_DIR")
    if env:
        return Path(env)
    sysname = sys.platform
    if sysname == "darwin":
        base = Path.home() / "Library" / "Caches"
    elif sysname.startswith("win"):
        base = Path(os.environ.get("LOCALAPPDATA", Path.home() / "AppData" / "Local"))
    else:
        base = Path(os.environ.get("XDG_CACHE_HOME", Path.home() / ".cache"))
    return base / "meshkore" / "chrome"


def _cache_bust(url: str) -> str:
    """Append a nonce so a stale edge/browser cache can't poison a verdict."""
    nonce = f"{_now_ms()}{os.getpid()}"
    sep = "&" if "?" in url else "?"
    return f"{url}{sep}_mkv={nonce}"


# ─────────────────────────────────────────────────────────────────────────────
# 1. browser discovery (zero install) + portable download fallback
# ─────────────────────────────────────────────────────────────────────────────

# Per-OS candidate paths, in preference order. Channel label is informational.
_MAC_CANDIDATES: Tuple[Tuple[str, str], ...] = (
    ("/Applications/Google Chrome.app/Contents/MacOS/Google Chrome", "chrome"),
    ("/Applications/Chromium.app/Contents/MacOS/Chromium", "chromium"),
    ("/Applications/Brave Browser.app/Contents/MacOS/Brave Browser", "brave"),
    ("/Applications/Microsoft Edge.app/Contents/MacOS/Microsoft Edge", "edge"),
    (
        "/Applications/Google Chrome Canary.app/Contents/MacOS/Google Chrome Canary",
        "chrome-canary",
    ),
)

# On Windows, Edge is part of the OS → a guaranteed zero-download engine.
_WIN_CANDIDATES: Tuple[Tuple[str, str], ...] = (
    (r"C:\Program Files\Google\Chrome\Application\chrome.exe", "chrome"),
    (r"C:\Program Files (x86)\Google\Chrome\Application\chrome.exe", "chrome"),
    (r"C:\Program Files (x86)\Microsoft\Edge\Application\msedge.exe", "edge"),
    (r"C:\Program Files\Microsoft\Edge\Application\msedge.exe", "edge"),
    (
        r"C:\Program Files\BraveSoftware\Brave-Browser\Application\brave.exe",
        "brave",
    ),
)

_LINUX_NAMES: Tuple[Tuple[str, str], ...] = (
    ("google-chrome", "chrome"),
    ("google-chrome-stable", "chrome"),
    ("chromium", "chromium"),
    ("chromium-browser", "chromium"),
    ("brave-browser", "brave"),
    ("microsoft-edge", "edge"),
)


def discover_browser(browser_dir: Optional[Path] = None) -> Optional[Tuple[str, str]]:
    """Return (executable_path, channel) for a host-installed Chromium-family
    browser, or None. Honours $MESHKORE_CHROME / $CHROME overrides first."""
    override = os.environ.get("MESHKORE_CHROME") or os.environ.get("CHROME")
    if override and Path(override).exists():
        return (override, "override")
    sysname = sys.platform
    if sysname == "darwin":
        for path, channel in _MAC_CANDIDATES:
            if Path(path).exists():
                return (path, channel)
    elif sysname.startswith("win"):
        # Also probe the per-user install location for Chrome.
        local = os.environ.get("LOCALAPPDATA", "")
        extra = (
            ((rf"{local}\Google\Chrome\Application\chrome.exe", "chrome"),)
            if local
            else ()
        )
        for path, channel in (*_WIN_CANDIDATES, *extra):
            if Path(path).exists():
                return (path, channel)
    else:
        for name, channel in _LINUX_NAMES:
            found = shutil.which(name)
            if found:
                return (found, channel)
    # last resort: a previously-downloaded portable Chrome in our cache
    cached = _cached_chrome_path(browser_dir)
    if cached and cached.exists():
        return (str(cached), "chrome-for-testing")
    return None


# Chrome-for-Testing platform keys (the download API's naming).
_CFT_PLATFORM = {
    ("darwin", "arm64"): "mac-arm64",
    ("darwin", "x86_64"): "mac-x64",
    ("linux", "x86_64"): "linux64",
    ("win32", "amd64"): "win64",
    ("win32", "x86_64"): "win64",
}

_CFT_LATEST = (
    "https://googlechromelabs.github.io/chrome-for-testing/"
    "last-known-good-versions-with-downloads.json"
)


def _cft_platform_key() -> Optional[str]:
    machine = platform.machine().lower()
    if machine in ("aarch64",):
        machine = "arm64"
    if machine in ("amd64", "x86_64"):
        machine = "x86_64"
    sysname = "win32" if sys.platform.startswith("win") else sys.platform
    return _CFT_PLATFORM.get((sysname, machine)) or _CFT_PLATFORM.get(
        (sysname, "x86_64")
    )


def _cached_chrome_path(browser_dir: Optional[Path] = None) -> Optional[Path]:
    """Where a downloaded Chrome-for-Testing binary lives, if present."""
    root = _cache_root(browser_dir)
    if not root.exists():
        return None
    # the unzip leaves a chrome-<platform>/ dir holding the executable
    for child in sorted(root.glob("chrome-*")):
        for exe in (
            child
            / "Google Chrome for Testing.app"
            / "Contents"
            / "MacOS"
            / "Google Chrome for Testing",
            child / "chrome.exe",
            child / "chrome",
        ):
            if exe.exists():
                return exe
    return None


def ensure_browser(
    allow_download: bool = True, log=print, browser_dir: Optional[Path] = None
) -> Tuple[str, str]:
    """Return (path, channel) of a usable browser, downloading a portable
    Chrome-for-Testing build (no installer, no admin) into `browser_dir` if none
    is installed. Raises RuntimeError if unavailable and download is off."""
    found = discover_browser(browser_dir)
    if found:
        return found
    if not allow_download:
        raise RuntimeError(
            "no Chromium-family browser found and download disabled "
            "(set MESHKORE_CHROME or install Chrome/Edge/Chromium)"
        )
    path = _download_chrome_for_testing(log=log, browser_dir=browser_dir)
    return (str(path), "chrome-for-testing")


def _download_chrome_for_testing(log=print, browser_dir: Optional[Path] = None) -> Path:
    """Fetch a portable Chrome-for-Testing .zip and unpack into `browser_dir`
    (the daemon passes a dir inside `.meshkore/`). No installer, no admin, no
    PATH change — a self-contained directory we run directly. Returns the exe."""
    pkey = _cft_platform_key()
    if not pkey:
        raise RuntimeError(
            f"no Chrome-for-Testing build for {sys.platform}/{platform.machine()}"
        )
    # NOTE: the binary downloads + lands fine, but DRIVING headless
    # Chrome-for-Testing over the stdlib CDP-WebSocket is not yet reliable
    # (CfT 150's browser ws resets on Target.attachToTarget) — the robust
    # `--remote-debugging-pipe` transport is tracked as VRF8. Prefer a host
    # browser: Windows already ships Edge; on macOS/Linux install Chrome or
    # set MESHKORE_CHROME=/path/to/chrome.
    log(
        f"verify: no host browser — downloading Chrome-for-Testing ({pkey}); "
        "NOTE driving it headless is experimental (VRF8) — prefer a host browser"
    )
    with urllib.request.urlopen(_CFT_LATEST, timeout=30) as r:
        meta = json.loads(r.read().decode("utf-8"))
    downloads = meta["channels"]["Stable"]["downloads"]["chrome"]
    url = next(d["url"] for d in downloads if d["platform"] == pkey)
    root = _cache_root(browser_dir)
    root.mkdir(parents=True, exist_ok=True)
    zip_path = root / f"chrome-{pkey}.zip"
    with urllib.request.urlopen(url, timeout=300) as r, open(zip_path, "wb") as f:
        shutil.copyfileobj(r, f)
    with zipfile.ZipFile(zip_path) as z:
        z.extractall(root)
    zip_path.unlink(missing_ok=True)
    exe = _cached_chrome_path(browser_dir)
    if not exe:
        raise RuntimeError("Chrome-for-Testing unpacked but no executable found")
    if sys.platform != "win32":
        os.chmod(exe, 0o755)
    log(f"verify: Chrome-for-Testing ready at {exe}")
    return exe


# ─────────────────────────────────────────────────────────────────────────────
# 2. minimal Chrome DevTools Protocol client (stdlib WebSocket)
# ─────────────────────────────────────────────────────────────────────────────

_LAUNCH_FLAGS = (
    "--headless=new",
    "--disable-gpu",
    "--hide-scrollbars",
    "--no-first-run",
    "--no-default-browser-check",
    "--disable-extensions",
    "--disable-background-networking",
    "--disable-cache",
    "--disk-cache-size=1",
    # the browser loads untrusted/draft UIs — treat as hostile input
    "--no-sandbox",
    "--disable-dev-shm-usage",
)


class CDPError(RuntimeError):
    pass


class _WS:
    """Tiny RFC-6455 *client* over a raw socket (CDP speaks ws://, never
    wss:// for a local browser). Masked client frames, unmasked server frames."""

    def __init__(self, host: str, port: int, path: str, timeout: float = 30.0):
        self.sock = socket.create_connection((host, port), timeout=timeout)
        self.sock.settimeout(timeout)
        key = base64.b64encode(os.urandom(16)).decode()
        handshake = (
            f"GET {path} HTTP/1.1\r\n"
            f"Host: {host}:{port}\r\n"
            "Upgrade: websocket\r\n"
            "Connection: Upgrade\r\n"
            f"Sec-WebSocket-Key: {key}\r\n"
            "Sec-WebSocket-Version: 13\r\n\r\n"
        )
        self.sock.sendall(handshake.encode())
        resp = self._read_until(b"\r\n\r\n")
        if b" 101 " not in resp.split(b"\r\n", 1)[0]:
            raise CDPError(f"WebSocket upgrade failed: {resp[:80]!r}")
        self._buf = b""

    def _read_until(self, sep: bytes) -> bytes:
        data = b""
        while sep not in data:
            chunk = self.sock.recv(4096)
            if not chunk:
                break
            data += chunk
        return data

    def _recv_exact(self, n: int) -> bytes:
        while len(self._buf) < n:
            chunk = self.sock.recv(65536)
            if not chunk:
                raise CDPError("WebSocket closed by browser")
            self._buf += chunk
        out, self._buf = self._buf[:n], self._buf[n:]
        return out

    def send_text(self, text: str) -> None:
        payload = text.encode("utf-8")
        header = bytearray([0x81])  # FIN + text opcode
        mask_bit = 0x80
        n = len(payload)
        if n < 126:
            header.append(mask_bit | n)
        elif n < 65536:
            header.append(mask_bit | 126)
            header += struct.pack(">H", n)
        else:
            header.append(mask_bit | 127)
            header += struct.pack(">Q", n)
        mask = os.urandom(4)
        header += mask
        masked = bytes(b ^ mask[i % 4] for i, b in enumerate(payload))
        self.sock.sendall(bytes(header) + masked)

    def recv_text(self) -> Optional[str]:
        """Return the next text message, reassembling fragments; None on close."""
        chunks: List[bytes] = []
        while True:
            b0, b1 = self._recv_exact(2)
            fin = b0 & 0x80
            opcode = b0 & 0x0F
            length = b1 & 0x7F
            if length == 126:
                length = struct.unpack(">H", self._recv_exact(2))[0]
            elif length == 127:
                length = struct.unpack(">Q", self._recv_exact(8))[0]
            payload = self._recv_exact(length) if length else b""
            if opcode == 0x8:  # close
                return None
            if opcode == 0x9:  # ping → pong (no mask needed for our purpose)
                continue
            if opcode == 0xA:  # pong
                continue
            chunks.append(payload)
            if fin:
                return b"".join(chunks).decode("utf-8", "replace")

    def close(self) -> None:
        try:
            self.sock.sendall(b"\x88\x80" + os.urandom(4))  # masked close
        except OSError:
            pass
        try:
            self.sock.close()
        except OSError:
            pass


class Chrome:
    """Launch a headless browser, attach to a page target, speak CDP. One
    instance == one browser process; always use as a context manager so it is
    torn down (process killed, temp profile removed) no matter what."""

    def __init__(self, exe: str, log=print):
        self.exe = exe
        self.log = log
        self.proc: Optional[subprocess.Popen] = None
        self.profile = Path(tempfile.mkdtemp(prefix="mkv-profile-"))
        self._ws: Optional[_WS] = None
        self._page_ws: Optional[_WS] = None
        self._id = 0
        self._lock = threading.Lock()
        self.console: List[Dict[str, Any]] = []
        self.network: List[Dict[str, Any]] = []

    # ── lifecycle ────────────────────────────────────────────────────────
    def __enter__(self) -> "Chrome":
        self.launch()
        return self

    def __exit__(self, *exc) -> None:
        self.close()

    def launch(self) -> None:
        args = [
            self.exe,
            *_LAUNCH_FLAGS,
            "--remote-debugging-port=0",
            f"--user-data-dir={self.profile}",
            "about:blank",
        ]
        self.proc = subprocess.Popen(
            args, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL
        )
        self._ws = self._open_browser_ws()
        # open a page target and attach to it (flatten → one socket, sessionId)
        target = self._cmd("Target.createTarget", {"url": "about:blank"})
        self.target_id = target["targetId"]
        attach = self._cmd(
            "Target.attachToTarget",
            {"targetId": self.target_id, "flatten": True},
        )
        self.session_id = attach["sessionId"]
        for domain in ("Page", "Runtime", "Network", "Log"):
            self._cmd(f"{domain}.enable", session=self.session_id)

    def _open_browser_ws(self, deadline: float = 25.0) -> _WS:
        """Connect the browser-level CDP WebSocket as soon as the browser is
        ready, and HOLD it open.

        Chrome writes ``DevToolsActivePort`` (two lines: ``<port>`` then the
        browser ws path ``/devtools/browser/<id>``) once the endpoint is up. We
        connect using THOSE values directly and skip the ``/json/version`` HTTP
        round-trip — that extra step races a freshly-downloaded
        ``--headless=new`` browser, whose initial DevTools server is live for
        only ~100 ms during a process handoff; the lingering open WS keeps the
        session alive. Poll-and-connect in one tight loop so we land inside that
        window (host browsers are warm and connect on the first read)."""
        f = self.profile / "DevToolsActivePort"
        end = time.time() + deadline
        last: Optional[Exception] = None
        while time.time() < end:
            if self.proc and self.proc.poll() is not None:
                raise CDPError(f"browser exited early (code {self.proc.returncode})")
            try:
                lines = f.read_text().splitlines()
            except OSError:
                lines = []
            if len(lines) >= 2 and lines[0].strip().isdigit():
                port, path = int(lines[0].strip()), lines[1].strip()
                try:
                    return _WS("127.0.0.1", port, path)
                except (CDPError, OSError) as e:
                    last = e  # window may have closed mid-handoff — re-read + retry
            time.sleep(0.02)
        raise CDPError(f"timed out opening CDP WebSocket — {last}")

    def close(self) -> None:
        for ws in (self._ws,):
            if ws:
                ws.close()
        if self.proc and self.proc.poll() is None:
            self.proc.terminate()
            try:
                self.proc.wait(timeout=5)
            except subprocess.TimeoutExpired:
                self.proc.kill()
        shutil.rmtree(self.profile, ignore_errors=True)

    # ── command / event pump ─────────────────────────────────────────────
    def _cmd(
        self,
        method: str,
        params: Optional[Dict[str, Any]] = None,
        session: Optional[str] = None,
        timeout: float = 30.0,
    ) -> Dict[str, Any]:
        with self._lock:
            self._id += 1
            msg_id = self._id
            msg: Dict[str, Any] = {"id": msg_id, "method": method}
            if params:
                msg["params"] = params
            if session:
                msg["sessionId"] = session
            assert self._ws is not None
            self._ws.send_text(json.dumps(msg))
            end = time.time() + timeout
            while time.time() < end:
                raw = self._ws.recv_text()
                if raw is None:
                    raise CDPError("connection closed awaiting response")
                data = json.loads(raw)
                if data.get("id") == msg_id:
                    if "error" in data:
                        raise CDPError(f"{method}: {data['error']}")
                    return data.get("result", {})
                self._on_event(data)
            raise CDPError(f"timeout waiting for {method}")

    def cmd(
        self, method: str, params: Optional[Dict[str, Any]] = None
    ) -> Dict[str, Any]:
        """Public CDP call on the attached page session."""
        return self._cmd(method, params, session=self.session_id)

    def _on_event(self, data: Dict[str, Any]) -> None:
        method = data.get("method")
        params = data.get("params", {})
        if method == "Runtime.consoleAPICalled":
            level = params.get("type", "log")
            text = " ".join(
                str(a.get("value", a.get("description", "")))
                for a in params.get("args", [])
            )
            self.console.append({"level": level, "text": text[:500]})
        elif method == "Log.entryAdded":
            entry = params.get("entry", {})
            self.console.append(
                {
                    "level": entry.get("level", "info"),
                    "text": str(entry.get("text", ""))[:500],
                }
            )
        elif method == "Network.responseReceived":
            resp = params.get("response", {})
            self.network.append(
                {"url": resp.get("url", "")[:300], "status": resp.get("status", 0)}
            )

    def _drain_for(self, seconds: float) -> None:
        """Pump events for a fixed window (lets console/network buffers fill)."""
        assert self._ws is not None
        self._ws.sock.settimeout(seconds)
        end = time.time() + seconds
        try:
            while time.time() < end:
                raw = self._ws.recv_text()
                if raw is None:
                    break
                self._on_event(json.loads(raw))
        except (socket.timeout, OSError):
            pass
        finally:
            self._ws.sock.settimeout(30.0)

    # ── high-level page ops ──────────────────────────────────────────────
    def navigate(self, url: str, settle_ms: int = 2500) -> None:
        self.cmd("Page.navigate", {"url": url})
        self._drain_for(settle_ms / 1000.0)

    def set_viewport(
        self, w: int, h: int, dpr: float = 1.0, mobile: bool = False
    ) -> None:
        self.cmd(
            "Emulation.setDeviceMetricsOverride",
            {"width": w, "height": h, "deviceScaleFactor": dpr, "mobile": mobile},
        )

    def evaluate(self, expr: str) -> Any:
        res = self.cmd(
            "Runtime.evaluate",
            {"expression": expr, "returnByValue": True, "awaitPromise": True},
        )
        if res.get("exceptionDetails"):
            raise CDPError(f"eval: {res['exceptionDetails'].get('text', 'error')}")
        return res.get("result", {}).get("value")

    def screenshot(self, full_page: bool = True) -> bytes:
        params: Dict[str, Any] = {"format": "png"}
        if full_page:
            params["captureBeyondViewport"] = True
        res = self.cmd("Page.captureScreenshot", params)
        return base64.b64decode(res["data"])

    def apply_credentials(self, state: Dict[str, Any]) -> None:
        """Authenticate BEFORE navigation so credentialed pages are reachable.
        Three mechanisms (combine as needed) — no document required:
          state.headers      {name: value}        → Network.setExtraHTTPHeaders
          state.basic_auth   {username, password} → an Authorization: Basic … header
          state.cookies      [{name,value,domain|url,path,...}] → Network.setCookies
        Without these, a login-walled URL just renders the login page — Verify
        reports that honestly (it never bypasses auth)."""
        headers = dict(state.get("headers") or {})
        ba = state.get("basic_auth")
        if ba and ba.get("username") is not None:
            token = base64.b64encode(
                f"{ba.get('username', '')}:{ba.get('password', '')}".encode()
            ).decode()
            headers["Authorization"] = f"Basic {token}"
        if headers:
            self.cmd("Network.setExtraHTTPHeaders", {"headers": headers})
        cookies = state.get("cookies")
        if cookies:
            self.cmd("Network.setCookies", {"cookies": cookies})

    def set_state(self, state: Dict[str, Any]) -> None:
        """Seed localStorage before navigation-dependent assertions (needs a
        document — call AFTER an initial navigate to the target origin)."""
        ls = state.get("localStorage") or {}
        for k, v in ls.items():
            self.evaluate(f"localStorage.setItem({json.dumps(k)}, {json.dumps(v)})")


# ─────────────────────────────────────────────────────────────────────────────
# 3. the contract — verify(spec) -> evidence
# ─────────────────────────────────────────────────────────────────────────────

_DEFAULT_VIEWPORTS = [
    {"name": "mobile", "w": 390, "h": 844, "dpr": 2, "mobile": True},
    {"name": "desktop", "w": 1440, "h": 900, "dpr": 1, "mobile": False},
]

# layout heuristics run in-page; cheap, baseline-free.
_HEURISTICS_JS = """(() => {
  const de = document.documentElement;
  const overflow = de.scrollWidth > de.clientWidth + 1;
  let longest = 0;
  for (const el of document.querySelectorAll('p,li,span,a,h1,h2,h3,div')) {
    const t = (el.textContent || '').trim();
    if (t.length > longest && el.children.length === 0) longest = t.length;
  }
  return { horizontalOverflow: overflow, scrollWidth: de.scrollWidth,
           clientWidth: de.clientWidth, longestLine: longest,
           title: document.title || '' };
})()"""


def verify(
    spec: Dict[str, Any], out_dir: Optional[str] = None, log=print
) -> Dict[str, Any]:
    """Run visual + functional + api checks against `spec['url']` and return an
    evidence payload. Pure function: launch → act → capture → close.

    spec = {
      url, viewports?, steps?, capture?, state?,
      checks?: { visual?, functional?, api? }, settle_ms?, full_page?,
      allow_download?, inline_b64?, browser_dir?
    }
    state may carry localStorage / cookies / headers / basic_auth for
    credentialed pages (see Chrome.apply_credentials / set_state).
    """
    started = _now_ms()
    url = spec.get("url")
    if not url:
        raise ValueError("verify: spec.url is required")
    viewports = spec.get("viewports") or _DEFAULT_VIEWPORTS
    settle_ms = int(spec.get("settle_ms", 2500))
    full_page = bool(spec.get("full_page", True))
    inline_b64 = bool(spec.get("inline_b64", False))
    browser_dir = spec.get("browser_dir")
    out = Path(out_dir) if out_dir else Path(tempfile.mkdtemp(prefix="mkv-out-"))
    out.mkdir(parents=True, exist_ok=True)

    exe, channel = ensure_browser(
        allow_download=bool(spec.get("allow_download", True)),
        log=log,
        browser_dir=Path(browser_dir) if browser_dir else None,
    )
    log(f"verify: using {channel} at {exe}")

    shots: List[Dict[str, Any]] = []
    flows: List[Dict[str, Any]] = []
    api_results: List[Dict[str, Any]] = []
    layout: Dict[str, Any] = {}
    fail_reasons: List[str] = []

    busted = _cache_bust(url)

    with Chrome(exe, log=log) as browser:
        state = spec.get("state") or {}
        if state:
            # Credentials (cookies/headers/basic_auth) must be set BEFORE any
            # navigation so a login-walled URL loads authenticated.
            browser.apply_credentials(state)
            if state.get("localStorage"):
                # localStorage needs a document: navigate once, seed, then the
                # viewport loop re-navigates with the seed in place.
                browser.navigate(busted, settle_ms=300)
                browser.set_state(state)

        for vp in viewports:
            name = vp.get("name", f"{vp['w']}x{vp['h']}")
            browser.set_viewport(
                vp["w"], vp["h"], vp.get("dpr", 1), vp.get("mobile", False)
            )
            browser.navigate(busted, settle_ms=settle_ms)
            # heuristics on the desktop-ish pass inform the verdict
            try:
                h = browser.evaluate(_HEURISTICS_JS)
                if not layout:
                    layout = h
                if h.get("horizontalOverflow"):
                    fail_reasons.append(f"horizontal overflow @ {name}")
            except CDPError as e:
                log(f"verify: heuristics failed @ {name}: {e}")
            png = browser.screenshot(full_page=full_page)
            fname = f"{name}.png"
            (out / fname).write_bytes(png)
            shot = {"route": url, "viewport": name, "path": str(out / fname)}
            if inline_b64:
                shot["b64"] = base64.b64encode(png).decode()
            shots.append(shot)

        # functional flows (step DSL): goto/click/fill/waitFor/assert
        for flow in (spec.get("checks", {}) or {}).get("functional", []) or []:
            flows.append(_run_flow(browser, flow, busted, settle_ms, log))

    # api checks (no browser needed)
    for api in (spec.get("checks", {}) or {}).get("api", []) or []:
        api_results.append(_run_api(api, url))
        if not api_results[-1]["pass"]:
            fail_reasons.append(f"api {api_results[-1]['name']} failed")

    # console errors + 4xx/5xx network responses count against the verdict
    console_errors = [c for c in browser.console if c["level"] in ("error",)]
    bad_responses = [n for n in browser.network if n["status"] >= 400]
    if console_errors:
        fail_reasons.append(f"{len(console_errors)} console error(s)")
    if bad_responses:
        fail_reasons.append(f"{len(bad_responses)} response(s) ≥400")
    if any(not f["pass"] for f in flows):
        fail_reasons.append("functional flow failed")

    verdict = "pass" if not fail_reasons else "fail"
    payload = {
        "url": url,
        "verdict": verdict,
        "fail_reasons": fail_reasons,
        "shots": shots,
        "flows": flows,
        "api": api_results,
        "console": browser.console,
        "network": browser.network,
        "layout": layout,
        "channel": channel,
        "out_dir": str(out),
        "ms": _now_ms() - started,
    }
    (out / "report.json").write_text(json.dumps(payload, indent=2))
    return payload


def _run_flow(
    browser: "Chrome", flow: Dict[str, Any], base_url: str, settle_ms: int, log
) -> Dict[str, Any]:
    name = flow.get("flow") or flow.get("name") or "flow"
    evidence: List[str] = []
    ok = True
    try:
        for step in flow.get("steps", []):
            if "goto" in step:
                browser.navigate(_cache_bust(step["goto"]), settle_ms=settle_ms)
                evidence.append(f"goto {step['goto']}")
            elif "click" in step:
                sel = json.dumps(step["click"])
                clicked = browser.evaluate(
                    f"(() => {{ const el = document.querySelector({sel});"
                    f" if (!el) return false; el.click(); return true; }})()"
                )
                if not clicked:
                    ok = False
                    evidence.append(f"click MISS {step['click']}")
                    break
                browser._drain_for(0.4)
                evidence.append(f"click {step['click']}")
            elif "fill" in step:
                sel = json.dumps(step["fill"])
                val = json.dumps(step.get("value", ""))
                filled = browser.evaluate(
                    f"(() => {{ const el = document.querySelector({sel});"
                    f" if (!el) return false; el.value = {val};"
                    f" el.dispatchEvent(new Event('input',{{bubbles:true}}));"
                    f" return true; }})()"
                )
                if not filled:
                    ok = False
                    evidence.append(f"fill MISS {step['fill']}")
                    break
                evidence.append(f"fill {step['fill']}")
            elif "waitFor" in step:
                sel = json.dumps(step["waitFor"])
                deadline = time.time() + step.get("timeout_ms", 5000) / 1000.0
                seen = False
                while time.time() < deadline:
                    if browser.evaluate(f"!!document.querySelector({sel})"):
                        seen = True
                        break
                    browser._drain_for(0.2)
                ok = ok and seen
                evidence.append(
                    ("waitFor " if seen else "waitFor TIMEOUT ") + step["waitFor"]
                )
            elif "assert" in step:
                res = bool(browser.evaluate(f"!!({step['assert']})"))
                ok = ok and res
                evidence.append(
                    ("assert OK " if res else "assert FAIL ") + step["assert"]
                )
    except CDPError as e:
        ok = False
        evidence.append(f"error: {e}")
        log(f"verify: flow {name} errored: {e}")
    return {"name": name, "pass": ok, "evidence": evidence}


def _run_api(api: Dict[str, Any], base_url: str) -> Dict[str, Any]:
    name = api.get("name") or api.get("path") or api.get("url") or "api"
    method = api.get("method", "GET").upper()
    target = api.get("url")
    if not target:
        # join path against the page origin
        from urllib.parse import urljoin

        target = urljoin(base_url, api.get("path", "/"))
    expect = api.get("expect", 200)
    try:
        req = urllib.request.Request(target, method=method)
        ctx = ssl.create_default_context()
        with urllib.request.urlopen(req, timeout=15, context=ctx) as r:
            status = r.status
    except urllib.error.HTTPError as e:
        status = e.code
    except Exception as e:  # noqa: BLE001
        return {"name": name, "pass": False, "status": None, "error": str(e)[:200]}
    return {"name": name, "pass": status == expect, "status": status, "expect": expect}


# ─────────────────────────────────────────────────────────────────────────────
# 4. CLI — `python verify.py <url> [--out DIR] [--spec FILE] [--no-download]`
# ─────────────────────────────────────────────────────────────────────────────


def _main(argv: List[str]) -> int:
    import argparse

    ap = argparse.ArgumentParser(prog="verify", description="MeshKore Verify (local)")
    ap.add_argument("url", nargs="?", help="public/preview URL to verify")
    ap.add_argument("--out", default=None, help="output dir for shots + report.json")
    ap.add_argument("--spec", default=None, help="JSON spec file (overrides url-only)")
    ap.add_argument(
        "--no-download", action="store_true", help="never download a browser"
    )
    ap.add_argument("--b64", action="store_true", help="inline screenshots as base64")
    args = ap.parse_args(argv)

    if args.spec:
        spec = json.loads(Path(args.spec).read_text())
    elif args.url:
        spec = {"url": args.url}
    else:
        ap.error("provide a URL or --spec FILE")
        return 2
    if args.no_download:
        spec["allow_download"] = False
    if args.b64:
        spec["inline_b64"] = True

    result = verify(spec, out_dir=args.out)
    summary = {
        "verdict": result["verdict"],
        "fail_reasons": result["fail_reasons"],
        "shots": [s["path"] for s in result["shots"]],
        "out_dir": result["out_dir"],
        "ms": result["ms"],
    }
    print(json.dumps(summary, indent=2))
    return 0 if result["verdict"] == "pass" else 1


if __name__ == "__main__":
    sys.exit(_main(sys.argv[1:]))
