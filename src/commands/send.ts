/**
 * `meshcore send` — post a chat.user message to the local daemon.
 */
import path from 'node:path';
import { existsSync } from 'node:fs';

import { Runtime } from '../runtime.js';

interface SendOptions {
  text: string;
  conv?: string;
  author?: string;
}

export async function sendCmd(opts: SendOptions): Promise<void> {
  const meshkoreDir = path.resolve('.meshkore');
  if (!existsSync(meshkoreDir)) throw new Error('.meshkore/ not found');
  const runtime = new Runtime(meshkoreDir);
  const { alive, lock } = runtime.isServerAlive();
  if (!alive || !lock) {
    throw new Error('no server-mode daemon running — start one with `meshcore start`');
  }
  const r = await fetch(`http://localhost:${lock.port}/messages`, {
    method: 'POST',
    headers: {
      'Content-Type': 'application/json',
      'Authorization': `Bearer ${lock.token}`,
    },
    body: JSON.stringify({
      author: opts.author || lock.identity,
      conv: opts.conv,
      text: opts.text,
    }),
  });
  if (!r.ok) {
    throw new Error(`daemon rejected: ${r.status} ${await r.text()}`);
  }
  const body = await r.json();
  console.log(JSON.stringify(body, null, 2));
}
