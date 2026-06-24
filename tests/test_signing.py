"""Release-signature verification (py-1.27.5). The daemon must refuse to
auto-update to a build whose detached Ed25519 signature doesn't verify
against the pinned release key — this is what makes auto-update safe against
a CDN compromise / MITM. Covers the vendored crypto, the shipped bundle being
signed, and `verify_release_bundle`'s accept/refuse logic over real HTTP."""

from __future__ import annotations

import base64
import http.server
import socket
import sys
import threading
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
import bootupdate  # type: ignore[import-not-found]  # noqa: E402
import constants  # type: ignore[import-not-found]  # noqa: E402
from crypto_ed25519 import (  # type: ignore[import-not-found]  # noqa: E402
    ed25519_publickey,
    ed25519_sign,
    ed25519_verify,
)


# ── vendored crypto ────────────────────────────────────────────────────────


def test_sign_verify_roundtrip() -> None:
    seed = bytes(range(32))
    pub = ed25519_publickey(seed)
    msg = b"meshkore daemon bundle bytes" * 4000
    sig = ed25519_sign(msg, seed, pub)
    assert ed25519_verify(pub, msg, sig) is True
    assert ed25519_verify(pub, msg + b"x", sig) is False  # tampered message
    assert (
        ed25519_verify(pub, msg, bytes([sig[0] ^ 1]) + sig[1:]) is False
    )  # tampered sig
    other = ed25519_publickey(bytes(range(1, 33)))
    assert ed25519_verify(other, msg, sig) is False  # wrong key
    assert ed25519_verify(pub, msg, b"too short") is False  # malformed


# ── the SHIPPED bundle is signed correctly ─────────────────────────────────


def test_shipped_bundle_is_signed() -> None:
    """Every published build must carry a .sig that verifies against the
    pinned pubkey — otherwise key-pinned clusters can't auto-update to it."""
    bundle = ROOT / "dist" / "daemon.py"
    sig_file = ROOT / "dist" / "daemon.py.sig"
    if not bundle.exists() or not sig_file.exists():
        pytest.skip("no dist/ bundle built")
    assert constants.RELEASE_PUBKEY_HEX, "RELEASE_PUBKEY_HEX must be pinned"
    pub = bytes.fromhex(constants.RELEASE_PUBKEY_HEX)
    sig = base64.b64decode(sig_file.read_text().strip())
    assert ed25519_verify(pub, bundle.read_bytes(), sig) is True


# ── verify_release_bundle over real HTTP ────────────────────────────────────


def _serve(directory: Path) -> tuple[http.server.HTTPServer, int]:
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        port = s.getsockname()[1]
    handler = lambda *a, **k: http.server.SimpleHTTPRequestHandler(  # noqa: E731
        *a, directory=str(directory), **k
    )
    httpd = http.server.HTTPServer(("127.0.0.1", port), handler)
    threading.Thread(target=httpd.serve_forever, daemon=True).start()
    return httpd, port


@pytest.fixture
def signed_cdn(tmp_path: Path, monkeypatch: pytest.MonkeyPatch):
    """A localhost 'CDN' serving daemon.py (+ .sig) signed by a TEST key
    that we pin via monkeypatch — hermetic, independent of the real key."""
    seed = bytes(range(7, 39))
    pub = ed25519_publickey(seed)
    monkeypatch.setattr(bootupdate, "RELEASE_PUBKEY_HEX", pub.hex())
    payload = b"#!/usr/bin/env python3\nDAEMON_VERSION = 'py-9.9.9'\n"
    (tmp_path / "daemon.py").write_bytes(payload)
    (tmp_path / "daemon.py.sig").write_text(
        base64.b64encode(ed25519_sign(payload, seed, pub)).decode() + "\n"
    )
    httpd, port = _serve(tmp_path)
    yield tmp_path, port, payload
    httpd.shutdown()


def _url(port: int) -> str:
    return f"http://localhost:{port}/daemon.py"


def test_valid_signature_accepted(signed_cdn) -> None:
    tmp, port, payload = signed_cdn
    err = bootupdate.verify_release_bundle(payload, _url(port), timeout=5, ua="t")
    assert err is None


def test_tampered_payload_refused(signed_cdn) -> None:
    tmp, port, payload = signed_cdn
    err = bootupdate.verify_release_bundle(
        payload + b"# evil", _url(port), timeout=5, ua="t"
    )
    assert err and "does not verify" in err


def test_missing_signature_refused(signed_cdn) -> None:
    tmp, port, payload = signed_cdn
    (tmp / "daemon.py.sig").unlink()  # CDN attacker omits the sig
    err = bootupdate.verify_release_bundle(payload, _url(port), timeout=5, ua="t")
    assert err and "signature unavailable" in err


def test_empty_pinned_key_disables_enforcement(signed_cdn, monkeypatch) -> None:
    tmp, port, payload = signed_cdn
    monkeypatch.setattr(bootupdate, "RELEASE_PUBKEY_HEX", "")
    # No signature fetched at all when enforcement is off.
    assert (
        bootupdate.verify_release_bundle(payload, _url(port), timeout=5, ua="t") is None
    )
