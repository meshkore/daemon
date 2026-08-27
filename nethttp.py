"""nethttp.py — leaf module: the ONE outbound HTTP fetch helper.

DAH1 (initiative `daemon-audit-hardening`). Before this module, nine
call-sites across `bootstrap`, `bootupdate`, `selfupdate`, `selfupdatesvc`,
`render`, `scaffold`, `teamsvc`, `verify` and `verifysvc` each hand-rolled
the same four lines:

    req = urllib.request.Request(url, headers={"User-Agent": ...})
    with urllib.request.urlopen(req, timeout=N) as r:
        payload = r.read()

…with a different User-Agent spelling, a different timeout, no scheme
check, and no ceiling on what they were willing to read into memory. Each
one that gained a hardening rule gained it alone.

What this leaf adds on top of the shared shape:

- **Scheme allow-list.** `urlopen` happily follows `file://`, `ftp://` and
  anything else registered — for a daemon that fetches its OWN NEXT BINARY
  from a URL that comes partly from `cluster.yaml`, that is the wrong
  default. Only http/https here.
- **A read ceiling.** `r.read()` with no argument is unbounded; a hostile
  or broken origin could stream until the daemon OOMs. `max_bytes` caps it.
- **One User-Agent convention**, built from the caller's short label.

Deliberately dependency-free (stdlib only, no sibling imports) so it sits
near the top of `bundle.MODULES` and every layer above can use it. It does
NOT log: each caller already has its own contextual `_log` line and its own
idea of whether a failure is fatal, best-effort, or worth a stamp on disk.
"""

from __future__ import annotations

import urllib.error
import urllib.parse
import urllib.request
from typing import Dict, Optional

# Only these ever make sense for the daemon's outbound calls (CDN bundles,
# TLS material, the standard's version file, an LLM API). `file:`/`ftp:`/
# `data:` reaching urlopen from a config-derived URL is a bug, not a feature.
ALLOWED_SCHEMES = ("http", "https")

# Default ceiling on a single response body. dist/daemon.py is ~1 MB, the
# TLS bundle is a few KB, an LLM reply is well under this — 16 MB is roomy
# for every legitimate caller and still bounds a hostile origin.
DEFAULT_MAX_BYTES = 16 * 1024 * 1024


class FetchError(RuntimeError):
    """Any outbound-fetch failure: bad scheme, transport error, oversize."""


def _agent(label: str, version: str = "") -> str:
    """`meshcore-py/<version> <label>` — the convention every call-site was
    already spelling by hand, slightly differently each time."""
    return (
        f"meshcore-py/{version} {label}".strip() if version else f"meshcore-py {label}"
    )


def fetch_bytes(
    url: str,
    *,
    label: str,
    version: str = "",
    timeout: float = 10.0,
    max_bytes: int = DEFAULT_MAX_BYTES,
    headers: Optional[Dict[str, str]] = None,
    data: Optional[bytes] = None,
) -> bytes:
    """GET (or POST, when `data` is given) `url` and return the body.

    Raises `FetchError` on a disallowed scheme, any transport failure, or a
    body that exceeds `max_bytes`. Callers that treat a failure as
    best-effort should catch it — this helper never returns a partial or
    sentinel value, because "empty string" was already indistinguishable
    from "404 page" at several of the old call-sites.
    """
    scheme = (urllib.parse.urlsplit(url).scheme or "").lower()
    if scheme not in ALLOWED_SCHEMES:
        raise FetchError(f"refusing scheme {scheme or '(none)'!r} for {url!r}")
    hdrs = {"User-Agent": _agent(label, version)}
    if headers:
        hdrs.update(headers)
    req = urllib.request.Request(url, headers=hdrs, data=data)
    try:
        with urllib.request.urlopen(req, timeout=timeout) as r:  # noqa: S310
            # Read one byte past the cap so we can TELL oversize from exact-fit.
            payload = r.read(max_bytes + 1)
    except FetchError:
        raise
    except Exception as e:  # URLError, HTTPError, socket timeout, ssl, OSError
        raise FetchError(str(e)) from e
    if len(payload) > max_bytes:
        raise FetchError(f"response exceeds {max_bytes} bytes: {url!r}")
    return payload


def fetch_head_bytes(
    url: str,
    *,
    label: str,
    n: int,
    version: str = "",
    timeout: float = 10.0,
    headers: Optional[Dict[str, str]] = None,
) -> bytes:
    """First `n` bytes of `url`, asked for with a Range header and capped on
    read regardless of whether the origin honours it. Used by the version
    watcher, which only needs daemon.py's leading `DAEMON_VERSION = "..."`
    line, not the whole ~1 MB bundle."""
    hdrs = {"Range": f"bytes=0-{n - 1}"}
    if headers:
        hdrs.update(headers)
    return fetch_bytes(
        url,
        label=label,
        version=version,
        timeout=timeout,
        max_bytes=n,
        headers=hdrs,
    )


def fetch_text(
    url: str,
    *,
    label: str,
    version: str = "",
    timeout: float = 10.0,
    max_bytes: int = DEFAULT_MAX_BYTES,
    headers: Optional[Dict[str, str]] = None,
    encoding: str = "utf-8",
    errors: str = "replace",
) -> str:
    """`fetch_bytes` decoded. Same error contract."""
    return fetch_bytes(
        url,
        label=label,
        version=version,
        timeout=timeout,
        max_bytes=max_bytes,
        headers=headers,
    ).decode(encoding, errors=errors)
