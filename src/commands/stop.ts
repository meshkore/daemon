/**
 * `meshcore stop` — stop one or all daemons on this machine.
 */
import path from 'node:path';
import { existsSync } from 'node:fs';

import { Runtime } from '../runtime.js';
import { log } from '../lib/log.js';

interface StopOptions {
  identity?: string;
}

export async function stopCmd(opts: StopOptions): Promise<void> {
  const meshkoreDir = path.resolve('.meshkore');
  if (!existsSync(meshkoreDir)) {
    console.log('No .meshkore/ in this directory.');
    return;
  }
  const runtime = new Runtime(meshkoreDir);

  const targets = opts.identity
    ? [opts.identity]
    : runtime.listAgentPids().map(a => a.identity);

  if (!targets.length) {
    console.log('No daemons running.');
    return;
  }

  for (const id of targets) {
    const pid = runtime.readAgentPid(id);
    if (pid == null) {
      log.warn('no PID for identity', { identity: id });
      continue;
    }
    try {
      process.kill(pid, 'SIGTERM');
      console.log(`  ✓ SIGTERM ${id} (pid ${pid})`);
      // Wait briefly then SIGKILL if still alive
      setTimeout(() => {
        try { process.kill(pid, 0); process.kill(pid, 'SIGKILL'); } catch {}
      }, 1500);
    } catch (err: any) {
      log.warn('signal failed', { identity: id, pid, err: err?.message });
      runtime.clearAgentPid(id);
    }
  }

  // Clear server lock if its identity is being stopped (or all)
  const lock = runtime.readServerLock();
  if (lock && (!opts.identity || opts.identity === lock.identity)) {
    runtime.clearServerLock();
  }
}
