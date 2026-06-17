"""credapi.py — extracted from readapi.py (daemon-architecture-v2 Phase 3).

CredMixin: methods moved VERBATIM out of QueryMixin; Daemon inherits both so
every self.* still resolves on the combined instance -> byte-identical."""

from __future__ import annotations

import os
import re
from typing import Any, Dict, List, Optional, Tuple

from utils import _log

_CREDENTIAL_NAME_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._-]{0,63}$")

# Protected names cannot be written or deleted via the API. portal-token
# is the daemon's own auth secret — letting the cockpit overwrite it
# would lock the cockpit out of the daemon on the very next request.
CREDENTIAL_PROTECTED_NAMES = frozenset({"portal-token"})


def _validate_credential_name(name: str) -> Optional[Tuple[int, Dict[str, Any]]]:
    """Returns None when the name is OK, or a (code, body) error tuple
    ready to ship back to the client. Used by every credential CRUD
    endpoint as the first gate."""
    if not isinstance(name, str) or not name:
        return 400, {"error": "credential name required"}
    if not _CREDENTIAL_NAME_RE.match(name):
        return 400, {
            "error": "invalid credential name; allowed: A-Za-z0-9._- (≤64 chars, must start with alnum)",
        }
    if "/" in name or ".." in name:
        return 400, {"error": "path separators not allowed in credential name"}
    return None


class CredMixin:
    def credentials_listing(self) -> List[Dict[str, Any]]:
        """Names + sizes of every file in .meshkore/credentials/.
        Never the contents — the cockpit only needs to know what
        exists, never what's in them. Same security stance as Node."""
        if not self.paths.credentials.exists():
            return []
        out = []
        for f in sorted(self.paths.credentials.iterdir()):
            if f.name.startswith("."):
                continue
            try:
                size = f.stat().st_size if f.is_file() else None
            except OSError:
                size = None
            out.append(
                {
                    "name": f.name,
                    "size": size,
                    "is_symlink": f.is_symlink(),
                    # py-1.11.3 — protected names are listable but the
                    # cockpit's CRUD blocks edit/delete on them. portal-token
                    # is the canonical example: rewriting it from the cockpit
                    # would lock the cockpit out of its own daemon.
                    "protected": f.name in CREDENTIAL_PROTECTED_NAMES,
                }
            )
        return out

    def credential_read(self, name: str) -> Tuple[int, Dict[str, Any]]:
        """Return the credential value for the operator-facing reveal
        action. The cockpit's CredentialsBlock keeps values masked by
        default and only fetches the raw via this endpoint when the
        operator clicks 'reveal'. Auth-required (handled upstream)."""
        valid = _validate_credential_name(name)
        if valid is not None:
            return valid
        path = self.paths.credentials / name
        if not path.exists() or not path.is_file():
            return 404, {"error": "credential not found", "name": name}
        try:
            value = path.read_text(encoding="utf-8")
        except OSError as e:
            return 500, {"error": f"read failed: {e}"}
        return 200, {
            "name": name,
            "value": value,
            "protected": name in CREDENTIAL_PROTECTED_NAMES,
        }

    def credential_write(self, name: str, value: str) -> Tuple[int, Dict[str, Any]]:
        """Create or overwrite a credential file under .meshkore/credentials/.
        Always chmod 600. Refuses protected names (portal-token) so the
        cockpit can't accidentally lock itself out of the daemon."""
        valid = _validate_credential_name(name)
        if valid is not None:
            return valid
        if name in CREDENTIAL_PROTECTED_NAMES:
            return 403, {
                "error": "protected credential — managed by daemon",
                "name": name,
            }
        if not isinstance(value, str):
            return 400, {"error": "value must be a string"}
        self.paths.credentials.mkdir(parents=True, exist_ok=True)
        path = self.paths.credentials / name
        try:
            path.write_text(value, encoding="utf-8")
            os.chmod(path, 0o600)
        except OSError as e:
            return 500, {"error": f"write failed: {e}"}
        _log(f"credential written: {name} ({len(value)} bytes)")
        return 200, {"name": name, "size": len(value.encode("utf-8"))}

    def credential_delete(self, name: str) -> Tuple[int, Dict[str, Any]]:
        valid = _validate_credential_name(name)
        if valid is not None:
            return valid
        if name in CREDENTIAL_PROTECTED_NAMES:
            return 403, {
                "error": "protected credential — managed by daemon",
                "name": name,
            }
        path = self.paths.credentials / name
        if not path.exists():
            return 404, {"error": "credential not found", "name": name}
        try:
            path.unlink()
        except OSError as e:
            return 500, {"error": f"delete failed: {e}"}
        _log(f"credential deleted: {name}")
        return 200, {"deleted": True, "name": name}
