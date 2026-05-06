/**
 * `meshcore migrate-from-legacy` — convert a legacy `.meshkore` v1
 * single-file repo to the new `.meshkore/` folder layout.
 *
 * Conservative: backups everything, refuses to run if `.meshkore/`
 * folder already exists, prints a one-line warning if catalog
 * unreachable but proceeds with bundled defaults.
 */
import {
  existsSync,
  mkdirSync,
  readFileSync,
  renameSync,
  writeFileSync,
} from 'node:fs';
import path from 'node:path';
import YAML from 'yaml';

import { log } from '../lib/log.js';
import { Runtime } from '../runtime.js';

interface MigrateOptions {
  keepOld?: boolean;
  yes?: boolean;
  dryRun?: boolean;
}

export async function migrateFromLegacyCmd(opts: MigrateOptions): Promise<void> {
  const cwd = process.cwd();
  const legacyFile = path.join(cwd, '.meshkore');
  const legacyLocal = path.join(cwd, '.meshkore.local');
  const newDir = path.join(cwd, '.meshkore');

  // Detection
  if (!existsSync(legacyFile)) {
    throw new Error('no .meshkore file found in current directory');
  }
  // Check if it's already a directory (means already migrated)
  const stat = require('node:fs').statSync(legacyFile);
  if (stat.isDirectory()) {
    throw new Error('.meshkore is already a directory — already migrated');
  }

  console.log('\n  Detected legacy .meshkore single-file format.\n');

  // Parse legacy
  let legacy: any;
  try {
    legacy = JSON.parse(readFileSync(legacyFile, 'utf8'));
  } catch (err: any) {
    throw new Error(`failed to parse .meshkore: ${err.message}`);
  }
  if (legacy.meshkore_version !== 1) {
    log.warn(`unexpected meshkore_version: ${legacy.meshkore_version}; proceeding anyway`);
  }

  let legacyLocalData: any = null;
  if (existsSync(legacyLocal)) {
    try {
      legacyLocalData = JSON.parse(readFileSync(legacyLocal, 'utf8'));
    } catch {/* ignore */}
  }

  // Map fields
  const clusterId = legacy.cluster?.channel_id || legacy.cluster?.name?.replace(/[^a-z0-9-]/gi, '-').toLowerCase() || `${path.basename(cwd)}-cluster`;
  const clusterType = legacy.cluster ? 'dev' : 'comms';

  const newCluster = {
    version: 1,
    id: clusterId,
    type: clusterType,
    name: legacy.cluster?.name || legacy.directory?.agent_id || path.basename(cwd),
    description: legacy.cluster?.purpose || legacy.directory?.description || '',
    transport: {
      protocol: 'websocket',
      endpoint: 'wss://hub.meshkore.com/ws',
      fallback: legacy.hub?.url || 'https://hub.meshkore.com',
    },
    bootstrap: {
      hub: legacy.hub?.url || 'https://hub.meshkore.com',
      docs: legacy.hub?.docs || 'https://hub.meshkore.com/platform/docs/agent',
      install: 'https://meshkore.com/cluster/install',
      operate: 'https://meshkore.com/cluster/operate',
      spec: 'https://meshkore.com/cluster/spec/v1',
      ...(legacy.cluster?.invite ? { invite: legacy.cluster.invite } : {}),
    },
    profile: {
      capabilities: legacy.directory?.capabilities ?? [],
      visible_in_directory: legacy.profile?.visible_in_directory ?? false,
    },
    portal: { port: 5570 },
    admission: {
      mode: legacy.cluster?.admission ?? 'pubkey',
      approval: 'manual',
      github_users: [],
      admission_token_lifetime: 3600,
      max_pending_requests: 50,
    },
    members: [],
  };

  // Plan
  console.log('  Migration plan:\n');
  console.log(`    .meshkore                          → .meshkore.legacy.json (backup)`);
  if (legacyLocalData) {
    console.log(`    .meshkore.local                    → .meshkore.local.legacy.json (backup)`);
  }
  console.log(`    [new]                              → .meshkore/public/cluster.yaml`);
  console.log(`    [new]                              → .meshkore/public/README.md`);
  console.log(`    [new]                              → .meshkore/agents/${legacy.directory?.agent_id || 'default'}.yaml`);
  console.log(`    [new]                              → .meshkore/credentials/portal-token`);
  if (legacyLocalData?.identity?.api_key) {
    console.log(`    api_key                            → .meshkore/credentials/${legacy.directory?.agent_id || 'default'}.env`);
  }
  console.log(`    .gitignore                         → updated (.meshkore/* + !.meshkore/public/)\n`);

  if (opts.dryRun) {
    console.log('  [dry-run] no files written.\n');
    return;
  }
  if (!opts.yes) {
    console.log('  Re-run with --yes to execute.\n');
    return;
  }

  // 1. Backups
  renameSync(legacyFile, path.join(cwd, '.meshkore.legacy.json'));
  if (legacyLocalData) renameSync(legacyLocal, path.join(cwd, '.meshkore.local.legacy.json'));

  // 2. Create new structure
  for (const d of ['public', 'docs', 'roadmap/tasks', 'roadmap/log',
                   'agents', 'credentials', 'scripts', 'portal',
                   'timeline', 'diagrams', '.runtime/agents']) {
    mkdirSync(path.join(newDir, d), { recursive: true });
  }

  // 3. Public files
  writeFileSync(path.join(newDir, 'public', 'cluster.yaml'), YAML.stringify(newCluster, { indent: 2, lineWidth: 100 }));
  writeFileSync(path.join(newDir, 'public', 'README.md'),
    `# ${newCluster.name} — MeshKore cluster\n\nMigrated from legacy .meshkore on ${new Date().toISOString().slice(0, 10)}.\n\nJoin: see https://meshkore.com/cluster/install\n`);

  // 4. Agent identity
  const agentId = legacy.directory?.agent_id || `${require('node:os').hostname()}-default`;
  const agentYaml: any = {
    identity: agentId,
    role: 'agent',
    client: 'claude-code',
    agent_role: 'developer',
    permissions: { edit: true, shell: true, network: 'open' },
  };
  if (legacyLocalData?.identity?.api_key) {
    const envPath = path.join(newDir, 'credentials', `${agentId}.env`);
    writeFileSync(envPath, `# Migrated from .meshkore.local on ${new Date().toISOString().slice(0, 10)}\nMESHKORE_AGENT_ID=${agentId}\nMESHKORE_API_KEY=${legacyLocalData.identity.api_key}\n`, { mode: 0o600 });
    agentYaml.credentials = `credentials/${agentId}.env`;
  }
  writeFileSync(path.join(newDir, 'agents', `${agentId}.yaml`), YAML.stringify(agentYaml));

  // 5. Token
  const runtime = new Runtime(newDir);
  runtime.getOrCreateToken();

  // 6. .gitignore
  const gitignorePath = path.join(cwd, '.gitignore');
  let gitignore = existsSync(gitignorePath) ? readFileSync(gitignorePath, 'utf8') : '';
  for (const line of ['.meshkore/*', '!.meshkore/public/', '.meshkore.legacy.json', '.meshkore.local.legacy.json']) {
    if (!gitignore.includes(line)) {
      if (gitignore && !gitignore.endsWith('\n')) gitignore += '\n';
      gitignore += line + '\n';
    }
  }
  writeFileSync(gitignorePath, gitignore);

  console.log(`\n  ✓ Migration complete.\n`);
  console.log(`    Cluster id:    ${newCluster.id}`);
  console.log(`    Identity:      ${agentId}`);
  console.log(`    Backup files:  .meshkore.legacy.json${legacyLocalData ? ', .meshkore.local.legacy.json' : ''}`);
  console.log(`\n  Next:`);
  console.log(`    meshcore start --identity ${agentId}`);
  console.log(`    open https://meshkore-web.pages.dev/portal\n`);
}
