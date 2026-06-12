"""Cluster path layout — the source of truth for every file the
daemon reads or writes inside ``.meshkore/``.

Pure data, no I/O at construction, no threading. The lowest-coupling
module in the package; everything that needs to touch the cluster's
disk layout takes a ``Paths`` instance via constructor injection.

TLS constants live here too because the daemon's TLS bundle resolver
(``_find_tls_bundle``) sits next to ``daemon.py``, not under
``.meshkore/`` — but the filenames are part of the layout contract."""

from __future__ import annotations

from pathlib import Path

TLS_BUNDLE_NAME = "tls"
TLS_CERT_FILENAME = "fullchain.pem"
TLS_KEY_FILENAME = "privkey.pem"


class Paths:
    def __init__(self, root: Path):
        self.root = root.resolve()
        self.meshkore = self.root / ".meshkore"
        self.public = self.meshkore / "public"
        self.cluster_yaml = self.public / "cluster.yaml"
        # Standard §13 — deployment links registry. Optional file; the
        # registry treats a missing file as { version: 1, modules: [] }.
        self.links_yaml = self.public / "links.yaml"
        # Standard §14 — protocols (reusable multi-scope runbooks).
        self.protocols_dir = self.meshkore / "protocols"
        self.protocols_log = self.protocols_dir / "log"
        self.credentials = self.meshkore / "credentials"
        self.token_file = self.credentials / "portal-token"
        self.runtime = self.meshkore / ".runtime"
        self.pid_file = self.runtime / "daemon.pid"
        self.port_file = self.runtime / "port"
        self.timeline_dir = self.meshkore / "timeline"
        self.modules_dir = self.meshkore / "modules"
        self.docs_dir = self.meshkore / "docs"
        # Standard v14 §3.5 — project-wide INVARIANT context tree. The
        # canonical layout (overview/product/stack/architecture/
        # constraints + glossary + decisions/ + criteria/). Served
        # read-only over /context (tree) + /context/<path> (file body)
        # for the cockpit's Context tab. Distinct from docs/context.md
        # (the legacy single-file form predating v14).
        self.context_dir = self.meshkore / "context"
        # py-1.9.0 — daily narrative logs (operator/Claude prose, one
        # file per day). Served read-only over /log/<YYYY-MM-DD>.md +
        # listed under /log so the cockpit Diary tab can lazy-page.
        self.log_dir = self.meshkore / "log"
        # py-1.2.0 — where /self-update writes daemon.py + daemon.py.bak.
        self.scripts_dir = self.meshkore / "scripts"
        self.roadmap_dir = self.meshkore / "roadmap"
        self.state_json = self.roadmap_dir / "state.json"
        self.agents_dir = self.meshkore / "agents"
        self.initiatives = self.roadmap_dir / "initiatives"
        # Cron scheduler (D-CRON-01..05). State file is gitignored
        # under .meshkore/.runtime/; the logs dir holds per-run captures.
        self.crons_state_path = self.runtime / "crons.json"
        self.crons_logs_dir = self.runtime / "logs" / "cron"
        # py-1.12.19 — chat-turn queue (Standard v16). Per-conv FIFO
        # at `.meshkore/queues/<conv>.json`. Gitignored runtime artifact
        # — operator's typed pending instructions don't belong in git.
        self.queues_dir = self.meshkore / "queues"
        # py-1.12.21 — chat uploads. Image / file attachments sent with
        # /chat/dispatch land here under YYYY-MM-DD/ buckets so the
        # cockpit can render thumbnails. Gitignored (operator inputs
        # may contain anything); retention via cluster.yaml.uploads
        # (default 30 days), opportunistic GC on every upload.
        self.uploads_dir = self.meshkore / "uploads"
