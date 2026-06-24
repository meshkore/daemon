"""crypto_ed25519.py — vendored pure-Python Ed25519 (sign + verify + keygen).

Why this exists (py-1.27.5). The daemon auto-updates by downloading a new
`daemon.py` from the CDN and re-exec'ing into it. Before this module the only
check was "does it parse + contain a DAEMON_VERSION marker" — so whoever
controls the CDN (or breaks TLS) could swap in arbitrary code that runs as the
operator. This adds CRYPTOGRAPHIC verification: every release is Ed25519-signed
with a private key that lives OFF the CDN (operator's machine only); the daemon
pins the PUBLIC key (constants.RELEASE_PUBKEY_HEX) and refuses to swap to a
build whose detached signature doesn't verify.

Stdlib-only by necessity — the daemon ships as a single zero-install `.py`, no
pip, so we can't use `cryptography`. This is the well-known public-domain
reference implementation (D. J. Bernstein et al., RFC 8032), with modular
exponentiation delegated to Python's built-in `pow(b, e, m)` for speed. Verify
runs once per update check (~0.1–0.5 s for two scalar-mults); fine for that.

NOT a general-purpose crypto library — it is constant-time-naive and only used
for release-signature verification of a public artifact. Do not repurpose it
for secrets handling.
"""

from __future__ import annotations

import hashlib

_b = 256
_q = 2**255 - 19
_L = 2**252 + 27742317777372353535851937790883648493


def _H(m: bytes) -> bytes:
    return hashlib.sha512(m).digest()


def _inv(x: int) -> int:
    return pow(x, _q - 2, _q)


_d = (-121665 * _inv(121666)) % _q
_I = pow(2, (_q - 1) // 4, _q)


def _xrecover(y: int) -> int:
    xx = (y * y - 1) * _inv(_d * y * y + 1)
    x = pow(xx, (_q + 3) // 8, _q)
    if (x * x - xx) % _q != 0:
        x = (x * _I) % _q
    if x % 2 != 0:
        x = _q - x
    return x


_By = (4 * _inv(5)) % _q
_Bx = _xrecover(_By)
_B = [_Bx % _q, _By % _q]


def _edwards(P, Q):
    x1, y1 = P
    x2, y2 = Q
    x3 = (x1 * y2 + x2 * y1) * _inv(1 + _d * x1 * x2 * y1 * y2)
    y3 = (y1 * y2 + x1 * x2) * _inv(1 - _d * x1 * x2 * y1 * y2)
    return [x3 % _q, y3 % _q]


def _scalarmult(P, e: int):
    # Iterative double-and-add (avoids deep recursion on a 253-bit scalar).
    Q = [0, 1]
    bits = []
    while e > 0:
        bits.append(e & 1)
        e >>= 1
    for bit in reversed(bits):
        Q = _edwards(Q, Q)
        if bit:
            Q = _edwards(Q, P)
    return Q


def _encodeint(y: int) -> bytes:
    return y.to_bytes(_b // 8, "little")


def _encodepoint(P) -> bytes:
    x, y = P
    val = y | ((x & 1) << (_b - 1))
    return val.to_bytes(_b // 8, "little")


def _bit(h: bytes, i: int) -> int:
    return (h[i // 8] >> (i % 8)) & 1


def _Hint(m: bytes) -> int:
    h = _H(m)
    return sum(2**i * _bit(h, i) for i in range(2 * _b))


def _secret_scalar(seed: bytes) -> int:
    h = _H(seed)
    return 2 ** (_b - 2) + sum(2**i * _bit(h, i) for i in range(3, _b - 2))


def ed25519_publickey(seed: bytes) -> bytes:
    """32-byte Ed25519 public key from a 32-byte seed (private key)."""
    if len(seed) != 32:
        raise ValueError("seed must be 32 bytes")
    A = _scalarmult(_B, _secret_scalar(seed))
    return _encodepoint(A)


def ed25519_sign(msg: bytes, seed: bytes, pub: bytes) -> bytes:
    """64-byte detached Ed25519 signature over msg."""
    h = _H(seed)
    a = _secret_scalar(seed)
    r = _Hint(h[_b // 8 : _b // 4] + msg)
    R = _scalarmult(_B, r)
    S = (r + _Hint(_encodepoint(R) + pub + msg) * a) % _L
    return _encodepoint(R) + _encodeint(S)


def _isoncurve(P) -> bool:
    x, y = P
    return (-x * x + y * y - 1 - _d * x * x * y * y) % _q == 0


def _decodeint(s: bytes) -> int:
    return int.from_bytes(s, "little")


def _decodepoint(s: bytes):
    y = int.from_bytes(s, "little") & ((1 << (_b - 1)) - 1)
    x = _xrecover(y)
    if x & 1 != _bit(s, _b - 1):
        x = _q - x
    P = [x, y]
    if not _isoncurve(P):
        raise ValueError("point not on curve")
    return P


def ed25519_verify(pub: bytes, msg: bytes, sig: bytes) -> bool:
    """True iff `sig` is a valid Ed25519 signature of `msg` under `pub`.
    Never raises — any malformed input returns False."""
    try:
        if len(sig) != 64 or len(pub) != 32:
            return False
        R = _decodepoint(sig[: _b // 8])
        A = _decodepoint(pub)
        S = _decodeint(sig[_b // 8 : _b // 4])
        h = _Hint(_encodepoint(R) + pub + msg)
        return _scalarmult(_B, S) == _edwards(R, _scalarmult(A, h))
    except Exception:
        return False
