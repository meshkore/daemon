# meshkore/daemon

[![Listed on MeshKore](https://meshkore.com/badge.svg)](https://meshkore.com)

Part of the **MeshKore** agent network — the open directory + protocol for AI agents at **[meshkore.com](https://meshkore.com)**.

The MeshKore Python daemon. Stdlib only, no `pip install`, no Node.
Runs in any folder that has a `.meshkore/` tree and exposes the
cluster's HTTP + WebSocket API. Source is a ~21K-line, 76-module
package (this repo); `bundle.py` inlines it into one flat,
self-contained `daemon.py` for distribution — operators only ever
`curl` and run that single file, never this repo directly.

**Current version:** the authoritative number is the `DAEMON_VERSION`
constant in `constants.py` and the live CDN copy at
`https://architect.meshkore.com/reference/cluster/scripts/daemon.py` — this line
is a convenience pointer, not the source of truth (don't hand-update it).

## Source-of-truth deployment

**This repo is THE source of truth**, but it's a 76-file package
(`daemon/*.py` + `agent_prompts/`, `clidrivers/`), not a single file —
`bundle.py` walks them in dependency order and inlines them into one
flat, self-contained script.

To publish a new version:

```bash
python3 bundle.py                 # writes dist/daemon.py + signs it (dist/daemon.py.sig)
cp dist/daemon.py dist/daemon.py.sig \
   ../architect/public/reference/cluster/scripts/
cd ../architect && git add public/reference/cluster/scripts/ \
   && git commit -m "chore(daemon): publish py-<version> signed bundle to cockpit CDN" \
   && git push   # then the normal architect Pages deploy publishes it
```

`architect/public/reference/cluster/scripts/` is the actual publishing
surface (committed, not gitignored — `git log` there is the release
history). 2026-07-24 correction: this section previously described an
OLDER mechanism (`.meshkore/scripts/pages-deploy.sh` copying into
`webapp/reference/cluster/scripts/`) that predates platform-v2's rebuild
of `webapp/` — that script is stale, still references the retired
70k-static-HTML model, and webapp's `public/reference/cluster/` has no
`scripts/` directory to copy into anymore. **Never edit the copy in
architect/public/reference — it gets overwritten by the next publish.**

Any new project (`meshcore init`) downloads `daemon.py` + the `tls/`
bundle from that URL. The `tls/` bundle is optional but recommended
— without it the daemon serves plain HTTP and the cockpit at
`architect.meshkore.com` can't open WebSockets to it.

## Run

```bash
mkdir -p tls
curl -fsSL https://architect.meshkore.com/reference/cluster/scripts/daemon.py     -o daemon.py
curl -fsSL https://architect.meshkore.com/reference/cluster/scripts/tls/fullchain.pem -o tls/fullchain.pem
curl -fsSL https://architect.meshkore.com/reference/cluster/scripts/tls/privkey.pem   -o tls/privkey.pem
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
