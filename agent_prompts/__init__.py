"""agent_prompts — per-agent-type prompt registry, split into one
fragment file per type (DA / daemon-architecture-v2). Assembles the
AGENT_PROMPTS dict from the fragments in the original order.

Bundler note: bundle.py inlines the fragment files (sorted) then this
__init__ LAST; the `from .X import AP_X` lines are stripped (the names
already live in the flat bundle namespace). prompts.py still does
`from agent_prompts import AGENT_PROMPTS` unchanged."""

from __future__ import annotations

from typing import Dict

from ._custom import AP_CUSTOM
from ._deploy import AP_DEPLOY
from ._db import AP_DB
from ._testing import AP_TESTING
from ._audit import AP_AUDIT
from ._docs import AP_DOCS
from ._roadmap_architect import AP_ROADMAP_ARCHITECT
from ._review import AP_REVIEW

AGENT_PROMPTS: Dict[str, Dict[str, str]] = {
    "custom": AP_CUSTOM,
    "deploy": AP_DEPLOY,
    "db": AP_DB,
    "testing": AP_TESTING,
    "audit": AP_AUDIT,
    "docs": AP_DOCS,
    "roadmap-architect": AP_ROADMAP_ARCHITECT,
    "review": AP_REVIEW,
}
