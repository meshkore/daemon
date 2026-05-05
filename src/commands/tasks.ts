/**
 * `meshcore tasks` — list tasks from .meshkore/roadmap/state.json.
 */
import path from 'node:path';
import { existsSync, readFileSync } from 'node:fs';
import chalk from 'chalk';

interface TasksOptions {
  status?: string; // comma-separated; default "next,in_progress"
  module?: string;
  limit?: number;
}

const STATUS_COLORS: Record<string, (s: string) => string> = {
  backlog:     chalk.gray,
  next:        chalk.yellow,
  in_progress: chalk.blue,
  blocked:     chalk.red,
  done:        chalk.green,
  cancelled:   chalk.dim,
};

export async function tasksCmd(opts: TasksOptions): Promise<void> {
  const meshkoreDir = path.resolve('.meshkore');
  const statePath = path.join(meshkoreDir, 'roadmap', 'state.json');
  if (!existsSync(statePath)) {
    console.log('state.json not found — run `python3 .meshkore/scripts/roadmap-build.py`');
    return;
  }
  const state = JSON.parse(readFileSync(statePath, 'utf8'));
  const tasks = (state.roadmap?.tasks ?? []) as any[];

  const filterStatuses = (opts.status ?? 'next,in_progress,blocked')
    .split(',')
    .map(s => s.trim())
    .filter(Boolean);

  let filtered = tasks.filter(t => filterStatuses.includes(t.status));
  if (opts.module) {
    filtered = filtered.filter(t => t.category === opts.module);
  }

  filtered.sort((a, b) => (a.category || '').localeCompare(b.category || '') || (a.id || '').localeCompare(b.id || ''));

  const limit = opts.limit ?? 80;
  const truncated = filtered.slice(0, limit);

  console.log(`\n  ${truncated.length} task(s)${filtered.length > limit ? ` (of ${filtered.length})` : ''}\n`);
  for (const t of truncated) {
    const color = STATUS_COLORS[t.status] ?? chalk.white;
    const status = color(t.status.padEnd(11));
    const id = chalk.cyan((t.id || '?').padEnd(8));
    const cat = chalk.gray((t.category || '-').padEnd(10));
    console.log(`  ${id} ${status} ${cat} ${t.title}`);
  }
  if (filtered.length > limit) {
    console.log(chalk.dim(`\n  … ${filtered.length - limit} more`));
  }
  console.log();
}
