# meshkore/daemon

The MeshKore Python daemon. Single-file (~3,900 lines), stdlib only,
no `pip install`, no Node. Runs in any folder that has a `.meshkore/`
tree and exposes the cluster's HTTP + WebSocket API.

## Source-of-truth deployment

This file is served live at:

    https://meshkore.com/reference/cluster/scripts/daemon.py

Any new project (`meshcore init`) downloads it from that URL. This
repo mirrors the source for transparency, forking and contributions.

## Run

```bash
curl -O https://meshkore.com/reference/cluster/scripts/daemon.py
python3 daemon.py
```

Binds the first free port in 5570–5589, serves the architect (HTTP +
WebSocket), rebuilds `state.json` from the markdown filesystem on
demand or on file change.

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
