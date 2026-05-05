/**
 * `meshcore init` — bootstrap .meshkore/ in the current repo.
 *
 * Steps:
 *  1. Validate we're inside a git repo.
 *  2. Pull templates from meshkore.com/reference/templates/.
 *  3. Create .meshkore/{public/cluster.yaml,public/README.md}.
 *  4. Update .gitignore.
 *  5. Pull scripts + portal from /reference/.
 *  6. Print next steps to the user.
 */
import { execSync } from 'node:child_process';

interface InitOptions {
  type: 'dev' | 'comms' | 'service' | 'mixed';
  id?: string;
  name?: string;
}

const REFERENCE_BASE = 'https://meshkore.com/reference';

export async function initCmd(opts: InitOptions): Promise<void> {
  // 1. git repo check
  try {
    execSync('git rev-parse --is-inside-work-tree', { stdio: 'ignore' });
  } catch {
    throw new Error('Not inside a git repository. Run `git init` first.');
  }

  // 2. Detect repo info
  const repoName = process.cwd().split('/').pop() ?? 'cluster';
  const clusterId = opts.id ?? `${repoName}-cluster`;
  const clusterName = opts.name ?? repoName;

  console.log(`Initializing cluster '${clusterId}' (type: ${opts.type})…`);

  // TODO: implement
  //   - mkdir -p .meshkore/{public,docs,roadmap,agents,credentials,scripts,portal}
  //   - fetch ${REFERENCE_BASE}/templates/cluster.yaml.${opts.type}
  //   - render placeholders, write to .meshkore/public/cluster.yaml
  //   - write .meshkore/public/README.md
  //   - update .gitignore (add `.meshkore/*\n!.meshkore/public/`)
  //   - download .meshkore/scripts/{roadmap-build,enrich-frontmatter}.py
  //   - download .meshkore/portal/index.html
  //   - generate portal-token (32 random bytes hex), save to credentials/

  console.log(`Will pull templates from: ${REFERENCE_BASE}`);
  console.log('TODO: implementation pending — see task C1 in roadmap.');
}
