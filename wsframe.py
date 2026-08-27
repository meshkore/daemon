"""wsframe.py — the RFC-6455 frame codec, written once (DAH5).

Three places in this daemon speak WebSocket, and each one carried its own
copy of the frame arithmetic:

    hub.WSClient.send_text   server → client writer, frames NOT masked
    routes._ws_read_frame    server ← client reader, frames ARE masked
    verify._WS               a full client (handshake, masked writes, reads)

They are three genuinely different halves of the protocol, so they stay three
classes — `WSClient` carries the per-connection `RLock` that fixed the native
heap corruption of py-1.31.4, and `_WS` is a masking client that reassembles
fragments. What was duplicated is only the codec: the FIN/opcode byte, the
three length encodings (7-bit / 16-bit / 64-bit), the mask key and the XOR.
That part is pure functions, and it lives here now.

The consolidation buys one thing beyond the line count: a payload ceiling in
ONE place. Both readers used to hand the wire-declared length straight to
their `recv_exact`, so a peer announcing a 2^63-byte payload had the process
growing a buffer until the socket died. `read_frame` refuses above
`max_payload` before a single byte of body is read.

Stdlib-only leaf: no sibling imports, so it inlines first in the bundle.
"""

from __future__ import annotations

import os
import struct
from typing import Callable, NamedTuple, Optional

# ── opcodes (RFC 6455 §5.2) ─────────────────────────────────────────────
OP_CONT = 0x0
OP_TEXT = 0x1
OP_BINARY = 0x2
OP_CLOSE = 0x8
OP_PING = 0x9
OP_PONG = 0xA

_FIN = 0x80
_MASK_BIT = 0x80

# 1 MiB. Every inbound frame this daemon actually expects is tiny — the
# cockpit sends nothing but close frames, and CDP replies are JSON — so the
# ceiling is a guard rail, not a tuning knob. Screenshot payloads travel the
# other way (browser → us) on the verify client, which is why it is not
# smaller: a full-page base64 PNG is legitimately large.
MAX_PAYLOAD_BYTES = 8 * 1024 * 1024


class FrameTooLarge(ValueError):
    """Peer declared a payload above the caller's ceiling. Raised BEFORE the
    body is read, so refusing costs nothing."""


class Frame(NamedTuple):
    opcode: int
    payload: bytes
    fin: bool


def mask_payload(data: bytes, key: bytes) -> bytes:
    """XOR `data` with the 4-byte `key`. Symmetric: masks and unmasks."""
    if not data or not key:
        return data
    return bytes(b ^ key[i % 4] for i, b in enumerate(data))


def encode(payload: bytes, *, opcode: int = OP_TEXT, mask: bool = False) -> bytes:
    """One unfragmented frame. `mask=True` for the client half (RFC 6455
    §5.3 requires client→server frames to be masked), `False` for the
    server half (which must NOT mask)."""
    header = bytearray()
    header.append(_FIN | opcode)
    bit = _MASK_BIT if mask else 0
    n = len(payload)
    if n < 126:
        header.append(bit | n)
    elif n < 65536:
        header.append(bit | 126)
        header.extend(struct.pack(">H", n))
    else:
        header.append(bit | 127)
        header.extend(struct.pack(">Q", n))
    if not mask:
        return bytes(header) + payload
    key = os.urandom(4)
    header.extend(key)
    return bytes(header) + mask_payload(payload, key)


def encode_text(text: str, *, mask: bool = False) -> bytes:
    return encode(text.encode("utf-8"), opcode=OP_TEXT, mask=mask)


def close_frame(*, mask: bool = False) -> bytes:
    return encode(b"", opcode=OP_CLOSE, mask=mask)


def read_frame(
    recv_exact: Callable[[int], bytes],
    *,
    max_payload: int = MAX_PAYLOAD_BYTES,
) -> Optional[Frame]:
    """Read exactly ONE frame off the wire and return it, or None at EOF.

    `recv_exact(n)` must return n bytes. The two callers disagree on how they
    signal EOF — the server reader returns b"", the verify client raises —
    and both work here: a short read becomes None, an exception propagates to
    the caller that owns the socket.

    Raises `FrameTooLarge` when the declared length exceeds `max_payload`.
    """
    hdr = recv_exact(2)
    if not hdr or len(hdr) < 2:
        return None
    b0, b1 = hdr[0], hdr[1]
    fin = bool(b0 & _FIN)
    opcode = b0 & 0x0F
    masked = bool(b1 & _MASK_BIT)
    length = b1 & 0x7F
    if length == 126:
        ext = recv_exact(2)
        if len(ext) < 2:
            return None
        length = struct.unpack(">H", ext)[0]
    elif length == 127:
        ext = recv_exact(8)
        if len(ext) < 8:
            return None
        length = struct.unpack(">Q", ext)[0]
    # Refuse BEFORE reading the body — that is the whole point of the check.
    if length > max_payload:
        raise FrameTooLarge(f"frame declares {length} bytes (max {max_payload})")
    key = recv_exact(4) if masked else b""
    if masked and len(key) < 4:
        return None
    payload = recv_exact(length) if length else b""
    if length and len(payload) < length:
        return None
    if masked:
        payload = mask_payload(payload, key)
    return Frame(opcode, payload, fin)
