"""yamlparse.py — tiny YAML + frontmatter parser.

Extracted from utils.py (daemon-architecture-v2 Phase 3d). Verbatim move;
imported back where used."""

from __future__ import annotations

import re
from typing import Any, Dict, List, Tuple

_FM_RE = re.compile(r"^---\s*\n(.*?)\n---\s*\n", re.DOTALL)


def parse_simple_yaml(text: str) -> Dict[str, Any]:
    """Parses a YAML subset sufficient for our cluster.yaml + frontmatter
    blocks. Supports scalars, dicts, lists, list-of-dicts, and inline
    list scalars (`tags: [a, b]`). NOT a general YAML parser — fail
    loudly for shapes we don't handle."""
    out: Dict[str, Any] = {}
    # Stack entry: (indent, container, key_in_parent, parent_ref_or_None)
    stack: List[Tuple[int, Any, str, Any]] = [(-1, out, "", None)]
    lines = text.splitlines()
    i = 0
    while i < len(lines):
        raw = lines[i]
        line = raw.rstrip()
        stripped = line.lstrip()
        if not stripped or stripped.startswith("#"):
            i += 1
            continue
        indent = len(line) - len(stripped)
        while stack and indent <= stack[-1][0] and len(stack) > 1:
            stack.pop()
        parent = stack[-1][1]

        if stripped.startswith("- "):
            value = stripped[2:].strip()
            # Promote: if the current container is an empty dict that was
            # just created as a nested holder for some key, convert it to
            # a list in the grandparent — we now know the value is a list.
            if isinstance(parent, dict) and not parent:
                key = stack[-1][2]
                gp = stack[-1][3]
                if key and isinstance(gp, dict) and gp.get(key) is parent:
                    new_list: List[Any] = []
                    gp[key] = new_list
                    stack[-1] = (stack[-1][0], new_list, key, gp)
                    parent = new_list
            if isinstance(parent, list):
                # Two shapes:
                #   "- value"               → scalar item
                #   "- key: val\n  key2: …" → dict item (continues below)
                if ":" in value:
                    item: Dict[str, Any] = {}
                    parent.append(item)
                    # Treat the inline "key: val" as the first dict entry
                    k2, _, v2 = value.partition(":")
                    k2 = k2.strip()
                    v2 = v2.strip()
                    if v2:
                        item[k2] = _coerce(_strip_inline_comment(v2))
                        stack.append((indent, item, "", parent))
                    else:
                        # Nested key with no value yet
                        nested: Dict[str, Any] = {}
                        item[k2] = nested
                        stack.append((indent, item, "", parent))
                        stack.append((indent + 2, nested, k2, item))
                else:
                    parent.append(
                        _coerce(_strip_inline_comment(value)) if value else None
                    )

        elif ":" in stripped:
            key, _, val = stripped.partition(":")
            key = key.strip()
            val = _strip_inline_comment(val.strip())
            if val == "":
                nxt: Dict[str, Any] = {}
                if isinstance(parent, dict):
                    parent[key] = nxt
                stack.append((indent, nxt, key, parent))
            elif val.startswith("[") and val.endswith("]"):
                # Inline list scalar: [a, b, "c d"]
                inner = val[1:-1].strip()
                items = (
                    [_coerce(x.strip()) for x in _split_top_level_commas(inner)]
                    if inner
                    else []
                )
                if isinstance(parent, dict):
                    parent[key] = items
            else:
                if isinstance(parent, dict):
                    parent[key] = _coerce(val)
        i += 1
    return out


def _strip_inline_comment(v: str) -> str:
    return re.sub(r"\s+#.*$", "", v)


def _split_top_level_commas(s: str) -> List[str]:
    out, buf, depth, in_str = [], "", 0, None
    for ch in s:
        if in_str:
            buf += ch
            if ch == in_str:
                in_str = None
            continue
        if ch in ('"', "'"):
            in_str = ch
            buf += ch
            continue
        if ch == "," and depth == 0:
            out.append(buf)
            buf = ""
            continue
        if ch in "[{":
            depth += 1
        elif ch in "]}":
            depth -= 1
        buf += ch
    if buf.strip():
        out.append(buf)
    return out


def _coerce(v: str) -> Any:
    s = v.strip()
    if not s:
        return ""
    if (s.startswith('"') and s.endswith('"')) or (
        s.startswith("'") and s.endswith("'")
    ):
        return s[1:-1]
    if s.lower() in ("true", "yes"):
        return True
    if s.lower() in ("false", "no"):
        return False
    if s.lower() in ("null", "~"):
        return None
    try:
        return int(s)
    except ValueError:
        pass
    try:
        return float(s)
    except ValueError:
        pass
    return s


def parse_frontmatter(text: str) -> Dict[str, Any]:
    m = _FM_RE.match(text)
    if not m:
        return {}
    return parse_simple_yaml(m.group(1))
