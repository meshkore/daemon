/**
 * `meshcore peek` — stream events from the local daemon's WebSocket.
 */
import path from 'node:path';
import { existsSync } from 'node:fs';
import WebSocket from 'ws';
import chalk from 'chalk';

import { Runtime } from '../runtime.js';

export async function peekCmd(): Promise<void> {
  const meshkoreDir = path.resolve('.meshkore');
  if (!existsSync(meshkoreDir)) throw new Error('.meshkore/ not found');
  const runtime = new Runtime(meshkoreDir);
  const { alive, lock } = runtime.isServerAlive();
  if (!alive || !lock) {
    throw new Error('no server-mode daemon — start one with `meshcore start`');
  }

  const url = `ws://localhost:${lock.port}/events?token=${encodeURIComponent(lock.token)}`;
  const ws = new WebSocket(url);
  ws.on('open', () => console.error(chalk.gray(`connected ${url}`)));
  ws.on('message', (data) => {
    try {
      const ev = JSON.parse(data.toString());
      const ts = (ev.ts || '').slice(11, 19);
      const tag = chalk.cyan((ev.type || '?').padEnd(20));
      console.log(`${chalk.gray(ts)}  ${tag} ${chalk.dim(JSON.stringify(ev).slice(0, 180))}`);
    } catch {
      console.log(data.toString());
    }
  });
  ws.on('close', () => { console.error(chalk.dim('disconnected')); process.exit(0); });
  ws.on('error', err => { console.error(chalk.red(String(err))); process.exit(1); });
  // Keep alive
  setInterval(() => {}, 1 << 30);
}
