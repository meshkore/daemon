# meshkore/daemon

[![Listed on MeshKore](https://meshkore.com/badge.svg)](https://meshkore.com)

Part of the **MeshKore** agent network — the open directory + protocol for AI agents at **[meshkore.com](https://meshkore.com)**.

The MeshKore Python daemon. Single-file (~4,500 lines), stdlib only,
no `pip install`, no Node. Runs in any folder that has a `.meshkore/`
tree and exposes the cluster's HTTP + WebSocket API.

**Current version:** `py-1.8.0` (loopback TLS via `daemon.meshkore.com`).

## Source-of-truth deployment

**This repo is THE source of truth.** The file is published to:

    https://meshkore.com/reference/cluster/scripts/daemon.py

via the deploy script (`pages-deploy.sh`) which copies `daemon.py`
+ `tls/` into `webapp/reference/cluster/scripts/` immediately before
the CF Pages deploy. The webapp tree is the publishing surface; the
daemon repo is where changes land first. **Never edit the copy in
webapp/reference — it gets overwritten.**

Any new project (`meshcore init`) downloads `daemon.py` + the `tls/`
bundle from that URL. The `tls/` bundle is optional but recommended
— without it the daemon serves plain HTTP and the cockpit at
`architect.meshkore.com` can't open WebSockets to it.

## Run

```bash
mkdir -p tls
curl -fsSL https://meshkore.com/reference/cluster/scripts/daemon.py     -o daemon.py
curl -fsSL https://meshkore.com/reference/cluster/scripts/tls/fullchain.pem -o tls/fullchain.pem
curl -fsSL https://meshkore.com/reference/cluster/scripts/tls/privkey.pem   -o tls/privkey.pem
chmod 600 tls/privkey.pem
python3 daemon.py
```

Binds the first free port in 5570–5589. With the bundle present,
serves HTTPS + WSS on `https://daemon.meshkore.com:<port>` (the
public DNS record `daemon.meshkore.com` resolves to 127.0.0.1, so
your localhost is reached via a real HTTPS origin without
mixed-content blocks or Chrome Local Network Access Issues).

Without the bundle, falls back to `http://localhost:<port>` —
works everywhere but a cockpit at `architect.meshkore.com` will
hit mixed-content rejections.

Requires Python ≥ 3.8 on macOS / Linux. No external dependencies.

## What it does

- Reads `.meshkore/` (tasks, docs, agents, credentials).
- Exposes the cluster's state at `GET /state` and a WebSocket at
  `/ws` for live events.
- Spawns headless LLM runners (claude-code, codex, etc.) on chat
  dispatch.
- Holds a wire-version contract via the `X-MeshKore-Daemon-Version`
  header so peer daemons can detect incompatibilities.
- Persists tool events, chat archives, timeline files atomically.

Full architecture: <https://meshkore.com/cluster/operate>.

## History

This repo started life as a Node implementation (`daemon/dist/` in the
operator monorepo) and was rewritten in pure Python under the
`unified-python-daemon` initiative (2026-05). The old Node tree is
preserved in the early commits of this history; the current single
`daemon.py` file at HEAD is the active code.

Some commit messages reference the Node-era paths (`daemon/src/...`,
`daemon/dist/...`) — those refer to where the file lived in the
monorepo before extraction.
