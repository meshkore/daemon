/**
 * State loader + file watcher.
 *
 * The Python script `roadmap-build.py` (in .meshkore/scripts/) does the heavy
 * lifting of building the canonical state.json from .meshkore/. The daemon
 * spawns it on demand and on file changes. Avoids reimplementing the parser
 * in two languages.
 *
 * Watched paths:
 *   .meshkore/timeline/*.jsonl    → triggers WS event broadcast (immediate)
 *   .meshkore/modules/            → triggers state rebuild (per-module tasks/log/diagrams/README)
 *   .meshkore/docs/               → triggers state rebuild (cross-cutting context)
 *   .meshkore/public/cluster.yaml → triggers state rebuild (modules block, identity)
 *   .meshkore/agents/             → triggers state rebuild (declared identities)
 *   (legacy) .meshkore/roadmap/tasks|log/  also watched for backward compat
 */
import { existsSync, openSync, readSync, closeSync, readFileSync, statSync } from 'node:fs';
import { spawn } from 'node:child_process';
import path from 'node:path';
import chokidar from 'chokidar';

import { log } from './lib/log.js';

export interface StateBundle {
  generated_at: string;
  cluster: Record<string, unknown>;
  members: unknown[];
  modules: unknown[];
  timeline: { days: unknown[]; recent_events: unknown[]; conversations: Record<string, unknown> };
  roadmap: { tasks: unknown[]; stats: Record<string, unknown> };
  docs: { tree: unknown[] };
}

export class StateManager {
  private cache: StateBundle | null = null;
  private buildPromise: Promise<StateBundle> | null = null;
  private debounceTimer: NodeJS.Timeout | null = null;

  constructor(
    public meshkoreDir: string,
    public onTimelineEvent: (ev: Record<string, unknown>) => void,
    public onStateRebuilt: (state: StateBundle) => void,
  ) {}

  // ─── State.json ─────────────────────────────────────────────────────────

  async getState(): Promise<StateBundle> {
    if (this.cache) return this.cache;
    return this.rebuild();
  }

  /** Spawn python script to regenerate state.json, cache result. */
  async rebuild(): Promise<StateBundle> {
    if (this.buildPromise) return this.buildPromise;
    this.buildPromise = this._rebuild();
    try {
      return await this.buildPromise;
    } finally {
      this.buildPromise = null;
    }
  }

  private async _rebuild(): Promise<StateBundle> {
    const script = path.join(this.meshkoreDir, 'scripts', 'roadmap-build.py');
    if (!existsSync(script)) {
      throw new Error(`roadmap-build.py not found at ${script}. Run \`meshcore init\` to install scripts.`);
    }
    log.debug('rebuilding state.json', { script });
    await new Promise<void>((resolve, reject) => {
      const p = spawn('python3', [script, '--meshkore', this.meshkoreDir, '--quiet'], {
        stdio: ['ignore', 'pipe', 'pipe'],
      });
      let stderr = '';
      p.stderr.on('data', d => { stderr += d.toString(); });
      p.on('error', reject);
      p.on('exit', code => {
        if (code === 0) resolve();
        else reject(new Error(`roadmap-build.py exit ${code}: ${stderr.slice(0, 500)}`));
      });
    });

    const statePath = path.join(this.meshkoreDir, 'roadmap', 'state.json');
    const state = JSON.parse(readFileSync(statePath, 'utf8')) as StateBundle;
    this.cache = state;
    this.onStateRebuilt(state);
    return state;
  }

  // ─── Watcher ────────────────────────────────────────────────────────────

  startWatcher() {
    const watch = [
      path.join(this.meshkoreDir, 'modules'),               // new layout: per-module folders
      path.join(this.meshkoreDir, 'docs'),                  // cross-cutting context
      path.join(this.meshkoreDir, 'agents'),
      path.join(this.meshkoreDir, 'public', 'cluster.yaml'),
      // Backward compat with the pre-v2 layout (still supported by the build script)
      path.join(this.meshkoreDir, 'roadmap', 'tasks'),
      path.join(this.meshkoreDir, 'roadmap', 'log'),
    ];
    const timelineDir = path.join(this.meshkoreDir, 'timeline');

    const watcher = chokidar.watch(watch, {
      ignoreInitial: true,
      persistent: true,
      ignored: /(^|[\/\\])\.[^.\/]/, // hidden files
    });
    watcher.on('all', (event, p) => {
      log.debug('fs change', { event, path: p });
      this.scheduleRebuild();
    });
    watcher.on('error', err => log.error('watcher error', { err: String(err) }));

    // Timeline gets its own watcher so we can stream new events to WS clients
    const timelineWatcher = chokidar.watch(timelineDir, {
      ignoreInitial: true,
      persistent: true,
    });
    const seen: Record<string, number> = {};
    const handleTimelineFile = (file: string) => {
      try {
        const stat = statSync(file);
        const lastSize = seen[file] ?? 0;
        if (stat.size <= lastSize) {
          seen[file] = stat.size;
          return;
        }
        const fd = openSync(file, 'r');
        const len = stat.size - lastSize;
        const buf = Buffer.alloc(len);
        readSync(fd, buf, 0, len, lastSize);
        closeSync(fd);
        seen[file] = stat.size;
        for (const line of buf.toString('utf8').split('\n')) {
          const trimmed = line.trim();
          if (!trimmed) continue;
          try {
            const ev = JSON.parse(trimmed);
            this.onTimelineEvent(ev);
          } catch {}
        }
        // Trigger state rebuild to update the recent_events section
        this.scheduleRebuild();
      } catch (err) {
        log.warn('failed to read timeline tail', { file, err: String(err) });
      }
    };
    timelineWatcher.on('add', f => { seen[f] = 0; handleTimelineFile(f); });
    timelineWatcher.on('change', f => handleTimelineFile(f));
  }

  private scheduleRebuild() {
    if (this.debounceTimer) clearTimeout(this.debounceTimer);
    this.debounceTimer = setTimeout(() => {
      this.rebuild().catch(err => log.error('rebuild failed', { err: String(err) }));
    }, 400);
  }
}
