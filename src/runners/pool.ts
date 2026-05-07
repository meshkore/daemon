/**
 * WorkerPool — persistent per-worker sessions on a single machine.
 *
 * Each worker is a long-lived "tab" of an AI client (Claude Code,
 * Codex, …) with:
 *   - a stable session id (UUID) that the underlying CLI honours via
 *     `--session-id <uuid>`, so consecutive dispatches resume the same
 *     conversation and accumulate context;
 *   - a fixed model (or `auto`);
 *   - an optional module binding (e.g. `api`, `portal`) so the
 *     coordinator can route a task to the worker that already has the
 *     module's context loaded;
 *   - a role (`coordinator` or `worker`).
 *
 * Persisted at `.meshkore/.runtime/workers.json`. The file is created
 * on first start with a single coordinator, and edited via the
 * portal's Network sub-panel (or the HTTP API).
 *
 * Note: V22 will add long-running interactive processes for true
 * multi-turn responsiveness. Today, persistence is achieved by passing
 * the same session id on each `claude -p` invocation.
 */
import { existsSync, readFileSync, writeFileSync, mkdirSync } from 'node:fs';
import path from 'node:path';
import crypto from 'node:crypto';

import { log } from '../lib/log.js';

export type WorkerKind = 'claude-code' | 'codex' | 'cursor' | 'deepseek' | 'qwen' | 'custom';
export type WorkerRole = 'coordinator' | 'worker';

/**
 * Permission policy passed to the underlying CLI:
 *  - 'safe'        : ask for every tool call. Useful when a human is
 *                    watching live (interactive). In headless dispatch
 *                    this WILL hang because no one can answer.
 *  - 'edits'       : auto-accept Edit/Write inside cwd; ask for the
 *                    rest (Bash, fetch, …). Reasonable middle ground.
 *  - 'unrestricted': skip every prompt — the agent operates fully
 *                    autonomously inside the repo cwd. Default for
 *                    headless workers; the cwd boundary is the
 *                    security envelope.
 */
export type WorkerPermissions = 'safe' | 'edits' | 'unrestricted';

export interface WorkerSpec {
  /** stable id (slug, lowercase) — e.g. "coordinator", "api-worker" */
  id: string;
  /** which AI client backs this worker */
  kind: WorkerKind;
  /** model to pass via --model. 'auto' = let the runner pick */
  model: string;
  /** stable session id for the underlying CLI */
  session_id: string;
  /** task scope: null = takes anything, otherwise a module id */
  module: string | null;
  /** coordinator (triages + delegates) or worker (executes) */
  role: WorkerRole;
  /** how the underlying CLI handles tool prompts */
  permissions?: WorkerPermissions;
  /** human-readable label for the portal */
  name?: string;
  /** epoch ms of the last dispatch this worker handled */
  last_used?: number;
  /** for portal display — manual notes */
  notes?: string;
}

export interface WorkerPoolFile {
  version: 1;
  workers: WorkerSpec[];
}

export class WorkerPool {
  private file: string;
  private data: WorkerPoolFile;

  constructor(meshkoreDir: string) {
    const runtimeDir = path.join(meshkoreDir, '.runtime');
    mkdirSync(runtimeDir, { recursive: true });
    this.file = path.join(runtimeDir, 'workers.json');
    this.data = this.load();
  }

  // ─── Persistence ────────────────────────────────────────────────────────

  private load(): WorkerPoolFile {
    if (!existsSync(this.file)) {
      const seeded: WorkerPoolFile = {
        version: 1,
        workers: [
          // Default: one coordinator that uses claude-code on the latest
          // sonnet. Replace via the portal Network panel if the user
          // doesn't have Anthropic creds.
          {
            id: 'coordinator',
            kind: 'claude-code',
            model: 'sonnet',
            session_id: crypto.randomUUID(),
            module: null,
            role: 'coordinator',
            permissions: 'unrestricted',
            name: 'Coordinator',
          },
        ],
      };
      writeFileSync(this.file, JSON.stringify(seeded, null, 2), { mode: 0o600 });
      return seeded;
    }
    try {
      const parsed = JSON.parse(readFileSync(this.file, 'utf8')) as WorkerPoolFile;
      if (!parsed.workers) throw new Error('missing workers[]');
      // Forward-compat: legacy workers had no `permissions` field. Fill
      // in 'unrestricted' (the only value that makes headless work) and
      // persist so the file matches what the runtime expects.
      let migrated = false;
      for (const w of parsed.workers) {
        if (!w.permissions) { w.permissions = 'unrestricted'; migrated = true; }
      }
      if (migrated) writeFileSync(this.file, JSON.stringify(parsed, null, 2), { mode: 0o600 });
      return parsed;
    } catch (err) {
      log.warn('workers.json malformed; resetting to defaults', { err: String(err) });
      const fallback: WorkerPoolFile = { version: 1, workers: [] };
      return fallback;
    }
  }

  private save() {
    writeFileSync(this.file, JSON.stringify(this.data, null, 2), { mode: 0o600 });
  }

  // ─── Public API ─────────────────────────────────────────────────────────

  list(): WorkerSpec[] {
    return this.data.workers.slice();
  }

  get(id: string): WorkerSpec | undefined {
    return this.data.workers.find(w => w.id === id);
  }

  /** Coordinator = the singleton worker tagged role:'coordinator'. If
   *  none exists (rare), fall back to the first declared worker. */
  coordinator(): WorkerSpec | undefined {
    return this.data.workers.find(w => w.role === 'coordinator') ?? this.data.workers[0];
  }

  /** Pick a worker for a given module. Prefers a module-scoped worker;
   *  falls back to the coordinator if none owns the module. */
  pickForModule(moduleId: string | null): WorkerSpec | undefined {
    if (moduleId) {
      const owner = this.data.workers.find(w => w.role === 'worker' && w.module === moduleId);
      if (owner) return owner;
    }
    return this.coordinator();
  }

  add(spec: Omit<WorkerSpec, 'session_id'> & { session_id?: string }): WorkerSpec {
    if (!spec.id) throw new Error('id is required');
    if (!/^[a-z0-9][a-z0-9-]{1,40}$/.test(spec.id)) throw new Error('id must be lowercase slug');
    if (this.data.workers.some(w => w.id === spec.id)) throw new Error(`worker ${spec.id} already exists`);
    const w: WorkerSpec = {
      // Default to 'unrestricted' — headless dispatches need to skip
      // prompts; the cwd boundary is the security envelope. The user can
      // tighten via the portal's worker edit dialog.
      permissions: 'unrestricted',
      ...spec,
      session_id: spec.session_id || crypto.randomUUID(),
    };
    this.data.workers.push(w);
    this.save();
    return w;
  }

  update(id: string, patch: Partial<Omit<WorkerSpec, 'id'>>): WorkerSpec {
    const i = this.data.workers.findIndex(w => w.id === id);
    if (i < 0) throw new Error(`worker ${id} not found`);
    this.data.workers[i] = { ...this.data.workers[i]!, ...patch };
    this.save();
    return this.data.workers[i]!;
  }

  remove(id: string): void {
    const i = this.data.workers.findIndex(w => w.id === id);
    if (i < 0) throw new Error(`worker ${id} not found`);
    if (this.data.workers[i]!.role === 'coordinator' && this.data.workers.filter(w => w.role === 'coordinator').length === 1) {
      throw new Error('cannot remove the only coordinator — promote another worker first');
    }
    this.data.workers.splice(i, 1);
    this.save();
  }

  /** Create a fresh session id for this worker (forgets prior context). */
  resetSession(id: string): WorkerSpec {
    return this.update(id, { session_id: crypto.randomUUID() });
  }

  touch(id: string) {
    const w = this.get(id);
    if (!w) return;
    w.last_used = Date.now();
    this.save();
  }
}
