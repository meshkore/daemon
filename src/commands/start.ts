/**
 * `meshcore start` — boot the daemon for one identity.
 *
 *   - First daemon binds <port> → server-mode (HTTP + WS + state + watcher)
 *   - Subsequent daemons → agent-only (no server, just publishes events)
 */
import path from 'node:path';
import { existsSync, readdirSync } from 'node:fs';
import { spawn } from 'node:child_process';
import os from 'node:os';
import process from 'node:process';

import { log } from '../lib/log.js';
import { acquirePort } from '../cluster/port-lock.js';
import { Runtime, type ServerLock } from '../runtime.js';
import { startServer } from '../server.js';
import { loadCluster } from '../state/cluster.js';

interface StartOptions {
  identity?: string;
  detach?: boolean;
  yolo?: boolean;
  port?: number;
}

export async function startCmd(opts: StartOptions): Promise<void> {
  const meshkoreDir = path.resolve('.meshkore');
  if (!existsSync(meshkoreDir)) {
    throw new Error('.meshkore/ not found — run `meshcore init` first');
  }

  const cluster = await loadCluster(meshkoreDir);
  const port = opts.port ?? cluster.portalPort ?? 5570;

  // Pick identity: arg, then first agents/*.yaml, then "default"
  let identity = opts.identity;
  if (!identity) {
    const agentsDir = path.join(meshkoreDir, 'agents');
    if (existsSync(agentsDir)) {
      const list = readdirSync(agentsDir).filter((f: string) => f.endsWith('.yaml'));
      if (list[0]) identity = list[0].replace(/\.yaml$/, '');
    }
  }
  if (!identity) {
    identity = `${os.hostname()}-default`;
    log.warn('no --identity given and no agents/*.yaml found; using default', { identity });
  }

  const runtime = new Runtime(meshkoreDir);
  runtime.ensure();

  // Detach? Re-spawn ourselves in the background and exit.
  if (opts.detach) {
    const args = process.argv.slice(1).filter(a => a !== '--detach');
    const child = spawn(process.execPath, args, {
      detached: true,
      stdio: 'ignore',
    });
    child.unref();
    console.log(`daemon started in background (pid ${child.pid})`);
    return;
  }

  // ─── Try to bind the port ──────────────────────────────────────────────
  const acquired = await acquirePort(port);

  if (!acquired.acquired) {
    log.info('server-mode port busy, running in AGENT-ONLY mode', { port, busyHost: acquired.heldBy ?? 'unknown' });
    runtime.writeAgentPid(identity, process.pid);
    handleSignals(() => runtime.clearAgentPid(identity!));
    // Agent-only loop: publish presence + wait
    log.info('agent-only loop running', { identity });
    keepAlive();
    return;
  }

  // ─── Server mode ───────────────────────────────────────────────────────
  log.info('server mode acquired port', { port, identity });
  const token = runtime.getOrCreateToken();

  const lock: ServerLock = {
    pid: process.pid,
    identity,
    port,
    started_at: new Date().toISOString(),
    token,
  };
  runtime.writeServerLock(lock);
  runtime.writeAgentPid(identity, process.pid);

  const server = await startServer({
    meshkoreDir,
    port,
    identity,
    token,
  });

  handleSignals(async () => {
    log.info('shutting down');
    await server.close();
    runtime.clearAgentPid(identity!);
    runtime.clearServerLock();
  });

  console.log(`\n  Daemon running on http://localhost:${port}`);
  console.log(`  Identity: ${identity}`);
  console.log(`  Token (paste into the portal once):`);
  console.log(`    ${token}`);
  console.log(`  Open: https://portal.meshkore.com  (or .meshkore/portal/index.html)\n`);

  keepAlive();
}

function handleSignals(cleanup: () => void | Promise<void>) {
  const handler = async (sig: string) => {
    log.info('signal', { sig });
    try { await cleanup(); } catch (e) { log.warn('cleanup error', { err: String(e) }); }
    process.exit(0);
  };
  for (const s of ['SIGINT', 'SIGTERM']) {
    process.on(s, () => handler(s));
  }
}

function keepAlive() {
  // Prevent process exit; we run until a signal arrives.
  setInterval(() => { /* heartbeat */ }, 1 << 30);
}
