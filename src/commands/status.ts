/**
 * `meshcore status` — show all daemons running on this machine.
 */
import path from 'node:path';
import { existsSync } from 'node:fs';

import { Runtime } from '../runtime.js';

export async function statusCmd(): Promise<void> {
  const meshkoreDir = path.resolve('.meshkore');
  if (!existsSync(meshkoreDir)) {
    console.log('No .meshkore/ in this directory.');
    return;
  }
  const runtime = new Runtime(meshkoreDir);
  const { alive, lock } = runtime.isServerAlive();

  console.log('\nMeshKore daemon status\n');
  if (alive && lock) {
    console.log(`  ✓ server-mode running`);
    console.log(`    pid:        ${lock.pid}`);
    console.log(`    identity:   ${lock.identity}`);
    console.log(`    port:       ${lock.port}`);
    console.log(`    started:    ${lock.started_at}`);
    console.log(`    health:     curl -fsS http://localhost:${lock.port}/health`);
  } else if (lock && !alive) {
    console.log(`  ⚠ stale lock detected (pid ${lock.pid} not running) — will be reclaimed on next start`);
  } else {
    console.log(`  · no server-mode daemon`);
  }

  const agents = runtime.listAgentPids();
  if (agents.length) {
    console.log(`\n  Agent processes:`);
    for (const a of agents) {
      const tag = a.alive ? '✓' : '⚠';
      console.log(`    ${tag} ${a.identity} (pid ${a.pid})${a.alive ? '' : ' [stale]'}`);
    }
  } else {
    console.log(`  · no agent identities running`);
  }
  console.log('');
}
