/**
 * `meshcore install-hooks` — symlink .meshkore/scripts/git-hooks/* into .git/hooks/.
 */
import path from 'node:path';
import { existsSync, readdirSync, statSync, symlinkSync } from 'node:fs';
import { execSync } from 'node:child_process';

import { log } from '../lib/log.js';

export async function installHooksCmd(): Promise<void> {
  const cwd = process.cwd();
  let gitRoot: string;
  try {
    gitRoot = execSync('git rev-parse --show-toplevel', { stdio: ['ignore', 'pipe', 'ignore'], encoding: 'utf8' }).trim();
  } catch {
    throw new Error('not inside a git repository');
  }

  const hooksSrc = path.join(cwd, '.meshkore', 'scripts', 'git-hooks');
  if (!existsSync(hooksSrc)) {
    throw new Error(`hooks source not found at ${hooksSrc} — run \`meshcore init\` first`);
  }
  const hooksDst = path.join(gitRoot, '.git', 'hooks');
  if (!existsSync(hooksDst)) throw new Error(`.git/hooks not found at ${hooksDst}`);

  let installed = 0;
  for (const f of readdirSync(hooksSrc)) {
    const src = path.join(hooksSrc, f);
    if (!statSync(src).isFile()) continue;
    const dst = path.join(hooksDst, f);
    if (existsSync(dst)) {
      console.log(`  · ${f} already exists in .git/hooks/ (skipping; remove it first to re-install)`);
      continue;
    }
    const rel = path.relative(hooksDst, src);
    symlinkSync(rel, dst);
    installed++;
    console.log(`  ✓ ${f} → ${rel}`);
  }
  console.log(`\n  Installed ${installed} hook(s).`);
  log.info('hooks installed', { count: installed, dst: hooksDst });
}
