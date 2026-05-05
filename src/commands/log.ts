/**
 * `meshcore log` — generate today's daily log from .meshkore/timeline/.
 * Wraps .meshkore/scripts/daily-log.py.
 */
import path from 'node:path';
import { existsSync } from 'node:fs';
import { spawn } from 'node:child_process';

import { log } from '../lib/log.js';

interface LogOptions {
  date?: string;
  since?: string;
  until?: string;
}

export async function logCmd(opts: LogOptions): Promise<void> {
  const meshkoreDir = path.resolve('.meshkore');
  if (!existsSync(meshkoreDir)) throw new Error('.meshkore/ not found');
  const script = path.join(meshkoreDir, 'scripts', 'daily-log.py');
  if (!existsSync(script)) throw new Error(`daily-log.py not found at ${script}`);

  const args = [script, '--meshkore', meshkoreDir];
  if (opts.date) args.push('--date', opts.date);
  if (opts.since) args.push('--since', opts.since);
  if (opts.until) args.push('--until', opts.until);

  await new Promise<void>((resolve, reject) => {
    const p = spawn('python3', args, { stdio: 'inherit' });
    p.on('error', reject);
    p.on('exit', code => code === 0 ? resolve() : reject(new Error(`exit ${code}`)));
  });
}
