/**
 * `meshcore init` — bootstrap .meshkore/ in the current repo.
 */
import { execSync } from 'node:child_process';
import {
  existsSync,
  mkdirSync,
  readFileSync,
  writeFileSync,
  appendFileSync,
} from 'node:fs';
import path from 'node:path';
import readline from 'node:readline/promises';
import { stdin, stdout } from 'node:process';

import { log } from '../lib/log.js';
import { makeCatalogClient, renderTemplate } from '../catalog.js';
import { Runtime } from '../runtime.js';

interface InitOptions {
  type: 'dev' | 'comms' | 'service' | 'mixed';
  id?: string;
  name?: string;
  description?: string;
  yes?: boolean;
}

export async function initCmd(opts: InitOptions): Promise<void> {
  // 1. Validate git repo
  try {
    execSync('git rev-parse --is-inside-work-tree', { stdio: 'ignore' });
  } catch {
    throw new Error('not inside a git repository — run `git init` first');
  }

  const cwd = process.cwd();
  const meshkoreDir = path.join(cwd, '.meshkore');

  if (existsSync(meshkoreDir)) {
    log.warn('.meshkore/ already exists — skipping creation, just refreshing scripts/portal');
  } else {
    log.info('creating .meshkore/ structure', { dir: meshkoreDir });
  }

  // Detect repo info for defaults
  let gitRepo = '';
  let gitBranch = 'main';
  try {
    gitRepo = execSync('git remote get-url origin 2>/dev/null', { encoding: 'utf8' }).trim();
  } catch {}
  try {
    gitBranch = execSync('git rev-parse --abbrev-ref HEAD 2>/dev/null', { encoding: 'utf8' }).trim() || 'main';
  } catch {}

  const repoName = path.basename(cwd);

  // Prompts (skipped if --yes or values provided)
  const rl = !opts.yes ? readline.createInterface({ input: stdin, output: stdout }) : null;
  const ask = async (q: string, def: string) => {
    if (!rl) return def;
    const ans = (await rl.question(`${q} [${def}]: `)).trim();
    return ans || def;
  };

  const id = opts.id || (rl ? await ask('cluster id', `${repoName}-cluster`) : `${repoName}-cluster`);
  const name = opts.name || (rl ? await ask('cluster name', repoName) : repoName);
  const description = opts.description || (rl ? await ask('description', `${repoName} cluster`) : `${repoName} cluster`);
  rl?.close();

  // 2. Create directory structure (module-centric v2 layout)
  for (const d of [
    'public', 'docs', 'modules', 'roadmap',
    'agents', 'credentials', 'scripts', 'portal',
    'timeline', 'log', '.runtime/agents',
  ]) {
    mkdirSync(path.join(meshkoreDir, d), { recursive: true });
  }
  // Seed a `general` catch-all module so the user can write tasks immediately
  for (const sub of ['tasks', 'log']) {
    mkdirSync(path.join(meshkoreDir, 'modules', 'general', sub), { recursive: true });
  }

  // 3. Pull templates from catalog → fill placeholders → write public/cluster.yaml
  const catalog = makeCatalogClient();
  const clusterYamlPath = path.join(meshkoreDir, 'public', 'cluster.yaml');
  if (!existsSync(clusterYamlPath)) {
    try {
      const tpl = await catalog.fetchText(`cluster/templates/cluster.yaml.${opts.type}`);
      const yaml = renderTemplate(tpl, {
        cluster_id: id,
        cluster_name: name,
        cluster_description: description,
        git_remote: gitRepo,
        git_branch: gitBranch,
        capabilities: '',
      });
      writeFileSync(clusterYamlPath, yaml);
      log.info('wrote public/cluster.yaml');
    } catch (err: any) {
      log.warn('catalog unreachable, writing minimal cluster.yaml', { err: err.message });
      writeFileSync(clusterYamlPath, minimalClusterYaml(id, name, description, opts.type, gitRepo, gitBranch));
    }
  } else {
    log.info('public/cluster.yaml already exists, leaving alone');
  }

  // 4. Public README
  const readmePath = path.join(meshkoreDir, 'public', 'README.md');
  if (!existsSync(readmePath)) {
    writeFileSync(readmePath, publicReadme(id, name));
  }

  // 5. .gitignore
  await ensureGitignore(cwd);

  // 6. Pull scripts from catalog
  for (const s of ['roadmap-build.py', 'enrich-frontmatter.py', 'timeline-append.py']) {
    const dest = path.join(meshkoreDir, 'scripts', s);
    if (existsSync(dest)) continue;
    try {
      await catalog.download(`cluster/scripts/${s}`, dest);
      try { execSync(`chmod +x ${dest}`); } catch {}
    } catch (err: any) {
      log.warn(`could not download ${s}`, { err: err.message });
    }
  }

  // 7. Pull portal HTML
  const portalDest = path.join(meshkoreDir, 'portal', 'index.html');
  if (!existsSync(portalDest)) {
    try {
      await catalog.download('cluster/portal/index.html', portalDest);
    } catch (err: any) {
      log.warn('could not download portal', { err: err.message });
    }
  }

  // 8. Generate token
  const runtime = new Runtime(meshkoreDir);
  const token = runtime.getOrCreateToken();

  // 9. Done — report
  log.info('cluster initialized', { id, type: opts.type });
  console.log('\n  Cluster initialized.\n');
  console.log(`  ID:        ${id}`);
  console.log(`  Type:      ${opts.type}`);
  console.log(`  Folder:    .meshkore/`);
  console.log(`  Portal token (paste into hosted portal once):`);
  console.log(`    ${token}\n`);
  console.log(`  Next steps:`);
  console.log(`    meshcore agent create --client claude-code --identity $(hostname)-claude --role developer`);
  console.log(`    meshcore start`);
  console.log(`    open .meshkore/portal/index.html  # or https://portal.meshkore.com\n`);
  console.log(`  ⚡ One thing to do once:\n`);
  console.log(`    Drop the right rules file at the repo root so any AI`);
  console.log(`    session in this repo follows the operator's manual:\n`);
  console.log(`      curl -fsSL https://meshkore.com/reference/cluster/editor-rules/CLAUDE.md     -o CLAUDE.md       # Claude Code`);
  console.log(`      curl -fsSL https://meshkore.com/reference/cluster/editor-rules/.cursorrules  -o .cursorrules    # Cursor`);
  console.log(`      curl -fsSL https://meshkore.com/reference/cluster/editor-rules/.windsurfrules -o .windsurfrules # Windsurf`);
  console.log(`\n  Operator manual:  https://meshkore.com/cluster/operate\n`);
}

async function ensureGitignore(cwd: string) {
  const gitignorePath = path.join(cwd, '.gitignore');
  const required = ['.meshkore/*', '!.meshkore/public/'];
  let content = '';
  if (existsSync(gitignorePath)) {
    content = readFileSync(gitignorePath, 'utf8');
  }
  let updated = false;
  for (const line of required) {
    if (!content.includes(line)) {
      if (content && !content.endsWith('\n')) content += '\n';
      content += line + '\n';
      updated = true;
    }
  }
  if (updated) {
    writeFileSync(gitignorePath, content);
    log.info('updated .gitignore with .meshkore/ rules');
  }
}

function minimalClusterYaml(id: string, name: string, desc: string, type: string, repo: string, branch: string): string {
  return `# .meshkore/public/cluster.yaml
# Generated locally (catalog unreachable). Replace with the canonical
# template from https://meshkore.com/reference/cluster/templates/ when online.

version: 1
id: ${id}
type: ${type}
name: "${name}"
description: "${desc}"

transport:
  protocol: websocket
  endpoint: wss://hub.meshkore.com/ws
  fallback: https://hub.meshkore.com

bootstrap:
  hub:     https://hub.meshkore.com
  docs:    https://hub.meshkore.com/platform/docs/agent
  install: https://meshkore.com/cluster/install
  spec:    https://meshkore.com/cluster/spec/v1

${type === 'dev' || type === 'mixed' ? `git:
  repo: ${repo || ''}
  branch: ${branch}
  auto_pull: true
  auto_commit: false

` : ''}portal:
  port: 5570
`;
}

function publicReadme(id: string, name: string): string {
  return `# ${name} — MeshKore cluster

This cluster's public surface. The full \`.meshkore/\` folder is local
on each member's machine; only this directory is committed.

## Join

1. Install the daemon: <https://meshkore.com/cluster/install>
2. \`meshcore init --join\` from the repo root
3. \`meshcore agent create\` for each AI client you'll use
4. \`meshcore start\` and open <https://portal.meshkore.com>

Cluster id: \`${id}\`
`;
}
