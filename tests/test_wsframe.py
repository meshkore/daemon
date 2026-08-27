"""DAH5 — the RFC-6455 codec, tested once instead of three times not at all.

`hub.WSClient`, `routes._ws_read_frame` and `verify._WS` each carried their
own copy of the frame arithmetic and none of them had a direct test: the
codec was only ever exercised end-to-end through a live socket. Extracting it
into pure functions makes the awkward parts — the three length encodings and
their boundaries, masking, fragmentation — cheap to pin down.

The ceiling matters most. Both readers used to pass the wire-declared length
straight to their `recv_exact`, so a peer announcing 2**63 bytes had the
process growing a buffer until the socket died.
"""

from __future__ import annotations

import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from wsframe import (  # noqa: E402
    OP_CLOSE,
    OP_PING,
    OP_TEXT,
    FrameTooLarge,
    close_frame,
    encode,
    encode_text,
    mask_payload,
    read_frame,
)


def _reader(data: bytes):
    """A `recv_exact` over an in-memory buffer, in the SERVER reader's dialect:
    a short read at EOF rather than an exception."""
    box = {"buf": data}

    def recv_exact(n: int) -> bytes:
        out, box["buf"] = box["buf"][:n], box["buf"][n:]
        return out

    return recv_exact


def _roundtrip(text: str, *, mask: bool) -> str:
    frame = read_frame(_reader(encode_text(text, mask=mask)))
    assert frame is not None
    assert frame.opcode == OP_TEXT
    assert frame.fin is True
    return frame.payload.decode("utf-8")


# ── the three length encodings, at their boundaries ─────────────────────


@pytest.mark.parametrize("n", [0, 1, 125, 126, 127, 65535, 65536, 70000])
@pytest.mark.parametrize("mask", [False, True])
def test_roundtrip_across_every_length_boundary(n: int, mask: bool) -> None:
    """125/126 and 65535/65536 are where the header switches from a 7-bit
    length to a 16-bit one and then to a 64-bit one — the arithmetic that was
    written out three times."""
    text = "x" * n
    assert _roundtrip(text, mask=mask) == text


def test_length_header_widths_are_the_spec_ones() -> None:
    assert encode_text("x" * 125)[1] & 0x7F == 125
    assert encode_text("x" * 126)[1] & 0x7F == 126  # → 16-bit extension
    assert encode_text("x" * 65536)[1] & 0x7F == 127  # → 64-bit extension


def test_utf8_survives_the_multibyte_case() -> None:
    """A length is in BYTES, not characters — an off-by-one here truncates
    mid-codepoint."""
    assert _roundtrip("piñón · 日本語 · 🛰", mask=True) == "piñón · 日本語 · 🛰"


# ── masking ─────────────────────────────────────────────────────────────


def test_server_frames_are_not_masked_and_client_frames_are() -> None:
    """RFC 6455 §5.3. Getting this backwards makes a browser hang up."""
    assert encode_text("hi", mask=False)[1] & 0x80 == 0
    assert encode_text("hi", mask=True)[1] & 0x80 == 0x80


def test_mask_is_symmetric_and_random_per_frame() -> None:
    key = b"\x01\x02\x03\x04"
    assert mask_payload(mask_payload(b"payload", key), key) == b"payload"
    # Two encodes of the same text must differ — a fixed key would leak
    # plaintext structure across frames.
    assert encode_text("same", mask=True) != encode_text("same", mask=True)


# ── the ceiling (the reason this extraction pays for itself) ────────────


def test_absurd_declared_length_is_refused_before_the_body_is_read() -> None:
    """A 64-bit length header claiming 2**63 bytes, and NO body behind it.
    The old readers handed that number straight to `recv_exact`."""
    header = b"\x81\x7f" + (2**63).to_bytes(8, "big")
    reads: list[int] = []

    def recv_exact(n: int) -> bytes:
        reads.append(n)
        return header[:n] if len(reads) == 1 else header[2:10]

    with pytest.raises(FrameTooLarge):
        read_frame(recv_exact)
    # Only the 2-byte head and the 8-byte extension were ever read.
    assert reads == [2, 8]


def test_ceiling_is_configurable_per_caller() -> None:
    frame = encode_text("x" * 1000)
    assert read_frame(_reader(frame), max_payload=2000) is not None
    with pytest.raises(FrameTooLarge):
        read_frame(_reader(frame), max_payload=500)


# ── control frames + fragmentation ──────────────────────────────────────


def test_close_frame_has_the_close_opcode_and_no_body() -> None:
    frame = read_frame(_reader(close_frame(mask=True)))
    assert frame is not None
    assert frame.opcode == OP_CLOSE
    assert frame.payload == b""


def test_fin_bit_distinguishes_a_fragment(cont: int = 0x0) -> None:
    """`verify._WS.recv_text` reassembles on this bit; if `fin` were always
    True it would return half a CDP reply and the JSON parse would fail."""
    first = bytearray(encode(b"half ", opcode=OP_TEXT, mask=False))
    first[0] &= 0x7F  # clear FIN
    frame = read_frame(_reader(bytes(first)))
    assert frame is not None and frame.fin is False
    tail = read_frame(_reader(encode(b"and half", opcode=cont, mask=False)))
    assert tail is not None and tail.fin is True


def test_ping_is_reported_not_swallowed() -> None:
    """The codec reports control opcodes; deciding to skip them is the
    caller's policy, and the two callers differ."""
    frame = read_frame(_reader(encode(b"", opcode=OP_PING, mask=True)))
    assert frame is not None and frame.opcode == OP_PING


# ── EOF ─────────────────────────────────────────────────────────────────


@pytest.mark.parametrize("truncated", [b"", b"\x81", b"\x81\x7e", b"\x81\x7e\x00"])
def test_eof_and_truncation_yield_none_not_a_crash(truncated: bytes) -> None:
    """A peer that dies mid-header must close the loop, not raise into the
    ws-pump's `except (OSError, ConnectionError)` which would not catch it."""
    assert read_frame(_reader(truncated)) is None


def test_body_shorter_than_the_declared_length_is_eof() -> None:
    assert read_frame(_reader(b"\x81\x05abc")) is None
