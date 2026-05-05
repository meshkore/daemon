# meshcore — MeshKore cluster daemon

Local CLI binary that turns a machine into a participant of a MeshKore
cluster. Reads `.meshkore/`, runs an LLM client (Claude Code, DeepSeek,
Qwen, Cursor) headless, exposes a localhost HTTP+WS API for the portal.

**Status: scaffolding.** Real implementation tracked in
[task C1](../.meshkore/roadmap/tasks/cluster/C1-meshcore-daemon.md).
This package is the structural starting point — interfaces and command
stubs only.

## Mental model

See [`cluster-install.html`](https://meshkore.com/cluster-install.html)
for the user-facing flow.

Architecture (canonical): [`docs/architecture/daemon.md`](../.meshkore/docs/architecture/daemon.md).

## Layout

```
daemon/
├── package.json
├── tsconfig.json
├── src/
│   ├── cli.ts                 entry point: command dispatcher
│   ├── commands/              one file per CLI command
│   │   ├── init.ts            meshcore init
│   │   ├── start.ts           meshcore start
│   │   ├── status.ts          meshcore status
│   │   ├── stop.ts            meshcore stop
│   │   ├── tasks.ts           meshcore tasks
│   │   └── agent.ts           meshcore agent create/list
│   ├── server/                localhost HTTP + WS API
│   │   ├── api.ts             REST routes
│   │   ├── events.ts          WS event stream
│   │   └── auth.ts            bearer token middleware
│   ├── clients/               LLM client adapters
│   │   ├── types.ts           AgentClient interface
│   │   ├── claude-code.ts
│   │   ├── deepseek.ts
│   │   ├── qwen.ts
│   │   ├── cursor.ts
│   │   └── custom.ts
│   ├── state/                 .meshkore/ readers/writers
│   │   ├── cluster.ts         load cluster.yaml
│   │   ├── agents.ts          load agents/*.yaml
│   │   ├── roadmap.ts         load tasks + log + state.json
│   │   └── docs.ts            load docs/ tree
│   ├── cluster/               transport to the cluster channel
│   │   ├── transport.ts       WebSocket connection to hub (or P2P)
│   │   ├── events.ts          cluster event types
│   │   └── port-lock.ts       server-mode vs agent-only detection
│   └── lib/                   helpers (git ops, logging, etc.)
└── test/
```

## Build & run

```bash
# Dev
npm install
npm run dev -- start --identity my-mac

# Compile to JS
npm run build

# Single binary (Mac/Linux/Windows via bun)
npm run build:bin
```

## Distribution targets

When v1 ships:

- npm: `npm install -g @meshkore/cli` (or `npx @meshkore/cli`).
- Homebrew: `brew install meshkore/tap/meshcore`.
- Scoop: `scoop install meshcore`.
- Direct binary: `https://download.meshkore.com/meshcore-{os}-{arch}`.

## License

MIT.
