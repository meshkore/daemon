/**
 * claude-code runner (V17 minimal).
 *
 * Spawns `claude -p <prompt>` headless from the repo root, captures
 * stdout/stderr, and emits timeline events back to the master daemon's
 * broadcast channel.
 *
 * This is intentionally small: it proves the daemon CAN exec the
 * AI client, hands back live progress, and finalises with
 * task.completed | task.failed. No git auto-commit, no streaming
 * deltas, no cancel — those come as the dispatcher matures.
 */
import { spawn } from 'node:child_process';
import { readFileSync, existsSync, statSync, readdirSync } from 'node:fs';
import path from 'node:path';

import { log } from '../lib/log.js';

/**
 * Read `.meshkore/credentials/<file>.env` (KEY=value, one per line) and
 * return it as a plain object suitable to merge into `process.env`.
 * Comments (`#`) and blank lines are ignored. Quoted values are
 * unquoted. Returns {} when the file doesn't exist.
 *
 * This is how worker spawns pick up `ANTHROPIC_API_KEY` for users
 * who haven't exported it globally — they put it in
 * `.meshkore/credentials/claude-code.env` and it travels into the
 * child process automatically.
 */
export function loadCredEnv(meshkoreDir: string, fileBase: string): Record<string, string> {
  const file = path.join(meshkoreDir, 'credentials', `${fileBase}.env`);
  if (!existsSync(file)) return {};
  const out: Record<string, string> = {};
  for (const raw of readFileSync(file, 'utf8').split(/\r?\n/)) {
    const line = raw.trim();
    if (!line || line.startsWith('#')) continue;
    const eq = line.indexOf('=');
    if (eq < 0) continue;
    const key = line.slice(0, eq).trim();
    let val = line.slice(eq + 1).trim();
    if ((val.startsWith('"') && val.endsWith('"')) || (val.startsWith("'") && val.endsWith("'"))) {
      val = val.slice(1, -1);
    }
    if (key) out[key] = val;
  }
  return out;
}

export interface RunOptions {
  meshkoreDir: string;                       // .../.meshkore
  taskId: string;                            // e.g. "V17"
  identity: string;                          // who is "running" the task
  /** binary to invoke; defaults to `claude` (must be in PATH) */
  bin?: string;
  /** flag passed before the prompt; defaults to `-p` */
  promptFlag?: string;
  /** stable claude session id — same UUID across dispatches keeps the
   *  conversation context (claude resumes it). When omitted the runner
   *  falls back to a fresh session per call. */
  sessionId?: string;
  /** model alias (e.g. 'sonnet', 'opus', 'haiku'). 'auto' / undefined
   *  lets claude pick its default. */
  model?: string;
  /** how strict the CLI is with tool-call prompts. 'unrestricted' is
   *  required for headless dispatches; otherwise claude blocks waiting
   *  for a human "approve write" answer that never arrives. */
  permissions?: 'safe' | 'edits' | 'unrestricted';
  /** sink for progress events. Master daemon should pass broadcast(). */
  emit: (event: Record<string, unknown>) => void;
}

export interface RunHandle {
  taskId: string;
  pid: number;
  startedAt: string;
  /** kill the subprocess (best-effort SIGTERM) */
  cancel(): void;
  /** resolves when the process exits; never rejects */
  done: Promise<{ exitCode: number; durationMs: number }>;
}

export function runClaudeCode(opts: RunOptions): RunHandle {
  const repoRoot = path.dirname(opts.meshkoreDir);
  const bin = opts.bin ?? 'claude';
  const promptFlag = opts.promptFlag ?? '-p';

  // Resolve task file: search every module's tasks/ + log/ for a file
  // whose stem starts with `<taskId>-`. Falls back to roadmap/tasks (legacy).
  const taskFile = findTaskFile(opts.meshkoreDir, opts.taskId);
  if (!taskFile) {
    throw new Error(`task ${opts.taskId} not found under .meshkore/modules/*/tasks or roadmap/tasks`);
  }
  const taskMd = readFileSync(taskFile, 'utf8');
  const prompt = buildPrompt(opts.taskId, taskMd, repoRoot);

  log.info('runner.claude-code spawning', { taskId: opts.taskId, bin, cwd: repoRoot, taskFile, sessionId: opts.sessionId, model: opts.model });

  const startedAt = new Date().toISOString();
  // Build CLI args. --session-id makes consecutive calls resume the same
  // claude conversation; --model pins the model the worker should use;
  // --permission-mode controls how prompts are answered headlessly.
  const args: string[] = [promptFlag];
  if (opts.sessionId) args.push('--session-id', opts.sessionId);
  if (opts.model && opts.model !== 'auto') args.push('--model', opts.model);
  // Map worker.permissions → claude --permission-mode value.
  // - 'unrestricted' → bypassPermissions (headless default; cwd-bound)
  // - 'edits'        → acceptEdits (auto edits, prompts for Bash etc.)
  // - 'safe'         → no flag → claude's default (prompts everything;
  //                    only useful when a human is at the other end)
  const permMode = opts.permissions === 'edits'        ? 'acceptEdits'
                 : opts.permissions === 'safe'         ? null
                 :                                       'bypassPermissions';
  if (permMode) args.push('--permission-mode', permMode);
  args.push(prompt);
  // Merge credentials from .meshkore/credentials/claude-code.env (and
  // optionally from a per-worker file <identity>.env) into the child
  // env. Without this, users who store ANTHROPIC_API_KEY in the env
  // file (instead of exporting it) get auth failures.
  const credEnv = {
    ...loadCredEnv(opts.meshkoreDir, 'claude-code'),
    ...loadCredEnv(opts.meshkoreDir, opts.identity),
  };
  // Spawn detached so the daemon doesn't get tangled in claude's TTY
  const child = spawn(bin, args, {
    cwd: repoRoot,
    env: { ...process.env, ...credEnv, MESHKORE_TASK: opts.taskId, MESHKORE_IDENTITY: opts.identity },
    stdio: ['ignore', 'pipe', 'pipe'],
  });

  opts.emit({
    type: 'task.started',
    id: opts.taskId,
    agent: opts.identity,
    ts: startedAt,
    runner: 'claude-code',
    bin,
    pid: child.pid,
  });

  // Stream stdout as task.progress (line-buffered, last line per chunk).
  let stdoutBuf = '';
  let stderrTail = '';
  child.stdout?.on('data', (chunk: Buffer) => {
    const text = chunk.toString('utf8');
    stdoutBuf += text;
    const lines = text.split(/\r?\n/);
    for (const line of lines) {
      const trimmed = line.trim();
      if (!trimmed) continue;
      opts.emit({
        type: 'task.progress',
        id: opts.taskId,
        agent: opts.identity,
        ts: new Date().toISOString(),
        line: trimmed.slice(0, 500),
      });
    }
  });
  child.stderr?.on('data', (chunk: Buffer) => {
    stderrTail = (stderrTail + chunk.toString('utf8')).slice(-2000);
  });

  const startMs = Date.now();
  const done = new Promise<{ exitCode: number; durationMs: number }>((resolve) => {
    child.on('exit', (code, signal) => {
      const durationMs = Date.now() - startMs;
      const exitCode = code ?? (signal ? 130 : 1);
      const summary = stdoutBuf.split(/\r?\n/).filter(Boolean).slice(-1)[0]?.slice(0, 300) || '';
      if (exitCode === 0) {
        opts.emit({
          type: 'task.completed',
          id: opts.taskId,
          agent: opts.identity,
          ts: new Date().toISOString(),
          runner: 'claude-code',
          duration_ms: durationMs,
          summary: summary || 'claude session ended',
        });
      } else {
        opts.emit({
          type: 'task.failed',
          id: opts.taskId,
          agent: opts.identity,
          ts: new Date().toISOString(),
          runner: 'claude-code',
          duration_ms: durationMs,
          exit_code: exitCode,
          error: stderrTail.slice(-300) || `exit ${exitCode}`,
        });
      }
      resolve({ exitCode, durationMs });
    });
    child.on('error', (err) => {
      // e.g. ENOENT when `claude` isn't on PATH
      opts.emit({
        type: 'task.failed',
        id: opts.taskId,
        agent: opts.identity,
        ts: new Date().toISOString(),
        runner: 'claude-code',
        error: `spawn ${bin}: ${err.message}`,
      });
      resolve({ exitCode: 127, durationMs: Date.now() - startMs });
    });
  });

  return {
    taskId: opts.taskId,
    pid: child.pid ?? -1,
    startedAt,
    cancel() { try { child.kill('SIGTERM'); } catch { /* noop */ } },
    done,
  };
}

function findTaskFile(meshkoreDir: string, taskId: string): string | null {
  const candidates: string[] = [];
  // New layout: modules/<id>/tasks/<taskId>-*.md and modules/<id>/log/<YYYY-MM>/<taskId>-*.md
  const modulesDir = path.join(meshkoreDir, 'modules');
  if (existsSync(modulesDir)) {
    for (const mod of readdirSync(modulesDir)) {
      const modPath = path.join(modulesDir, mod);
      if (!statSyncSafe(modPath)?.isDirectory()) continue;
      for (const sub of ['tasks', 'log']) {
        const subPath = path.join(modPath, sub);
        if (!existsSync(subPath)) continue;
        candidates.push(...walkForTaskFile(subPath, taskId));
      }
    }
  }
  // Legacy
  const legacy = path.join(meshkoreDir, 'roadmap');
  if (existsSync(legacy)) {
    for (const sub of ['tasks', 'log']) {
      const subPath = path.join(legacy, sub);
      if (existsSync(subPath)) candidates.push(...walkForTaskFile(subPath, taskId));
    }
  }
  return candidates[0] ?? null;
}

function walkForTaskFile(dir: string, taskId: string): string[] {
  const out: string[] = [];
  const stack = [dir];
  while (stack.length) {
    const d = stack.pop()!;
    let entries: string[] = [];
    try { entries = readdirSync(d); } catch { continue; }
    for (const e of entries) {
      const p = path.join(d, e);
      const st = statSyncSafe(p);
      if (!st) continue;
      if (st.isDirectory()) { stack.push(p); continue; }
      if (e.endsWith('.md') && (e === `${taskId}.md` || e.startsWith(`${taskId}-`))) {
        out.push(p);
      }
    }
  }
  return out;
}

function statSyncSafe(p: string) {
  try { return statSync(p); } catch { return null; }
}

function buildPrompt(taskId: string, taskMd: string, repoRoot: string): string {
  return [
    `You are running headless as part of a MeshKore cluster on this repo: ${repoRoot}`,
    `Task ${taskId} is in front of you. Read its frontmatter + body below; do the work.`,
    `When you finish, leave a brief summary as your last printed line.`,
    `Keep edits scoped, keep commits clean, do NOT push without explicit human approval.`,
    ``,
    `─── ${taskId} ───────────────────────────────`,
    taskMd.trim(),
    `─────────────────────────────────────────────`,
  ].join('\n');
}
