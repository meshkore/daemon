/**
 * Combined HTTP + WebSocket server for the daemon.
 *
 * Binds localhost:<port>. CORS allowlist for portal.meshkore.com,
 * localhost:*, and file:// origins.
 *
 * REST routes:
 *   GET  /health
 *   GET  /state
 *   GET  /state/{roadmap|docs|cluster|modules|timeline}
 *   GET  /docs/*                 raw markdown by path
 *   GET  /tasks/*                raw markdown by path
 *   POST /messages               append chat.user, broadcast
 *   POST /tasks/{id}/transition  update frontmatter, append task.transitioned
 *   GET  /agents                 declared local agents
 *   GET  /credentials            metadata only (filenames)
 *   GET  /reload                 force state rebuild
 *
 * WS:
 *   /events    streams cluster events as JSON lines
 */
import { createServer, IncomingMessage, ServerResponse } from 'node:http';
import { WebSocketServer, WebSocket } from 'ws';
import {
  existsSync,
  readFileSync,
  readdirSync,
  appendFileSync,
  mkdirSync,
  writeFileSync,
  statSync,
} from 'node:fs';
import path from 'node:path';

import { log } from './lib/log.js';
import { StateManager } from './state.js';
import { Runtime } from './runtime.js';
import { Admission, AdmissionRequestSchema } from './admission.js';
import { runClaudeCode, type RunHandle, loadCredEnv } from './runners/claude-code.js';
import { WorkerPool, type WorkerSpec } from './runners/pool.js';
import { createAgentIdentity } from './commands/agent.js';
import { spawn } from 'node:child_process';
import os from 'node:os';

// Per-host info shared with the portal so workers can display where
// they live (one Mac vs. another, hostname-aware). Read once at boot
// — hostname doesn't change frequently and we don't want syscalls
// on every request.
let _device: { hostname: string; platform: string; arch: string; os_release: string } | null = null;
function deviceInfo() {
  if (_device) return _device;
  _device = {
    hostname: os.hostname() || 'unknown',
    platform: os.platform(),
    arch: os.arch(),
    os_release: os.release(),
  };
  return _device;
}

// Active runs keyed by task id. One run per task at a time (later: queue).
const activeRuns = new Map<string, RunHandle>();

const ALLOWED_ORIGINS = [
  /^https:\/\/portal\.meshkore\.com$/,
  /^https:\/\/meshkore-portal\.pages\.dev$/,
  /^https:\/\/[a-f0-9]+\.meshkore-portal\.pages\.dev$/,
  /^https:\/\/meshkore-web\.pages\.dev$/,
  /^https:\/\/[a-f0-9]+\.meshkore-web\.pages\.dev$/,
  /^https:\/\/meshkore\.com$/,
  /^http:\/\/localhost(:\d+)?$/,
  /^http:\/\/127\.0\.0\.1(:\d+)?$/,
];

export interface ServerOptions {
  meshkoreDir: string;
  port: number;
  identity: string;
  token: string;
  bindAddress?: string;
}

export interface DaemonServer {
  close: () => Promise<void>;
  broadcast: (event: Record<string, unknown>) => void;
}

export async function startServer(opts: ServerOptions): Promise<DaemonServer> {
  const runtime = new Runtime(opts.meshkoreDir);
  const wsClients = new Set<WebSocket>();

  const broadcast = (event: Record<string, unknown>) => {
    const data = JSON.stringify(event);
    for (const client of wsClients) {
      if (client.readyState === WebSocket.OPEN) {
        try { client.send(data); } catch {}
      }
    }
  };

  const state = new StateManager(
    opts.meshkoreDir,
    (ev) => broadcast(ev),
    () => broadcast({ type: 'state.rebuilt', ts: new Date().toISOString() }),
  );

  // Persistent worker pool — one coordinator + N module-bound workers.
  // Each worker has a stable session_id so consecutive dispatches resume
  // the same Claude/Codex/etc. conversation instead of starting fresh.
  const workers = new WorkerPool(opts.meshkoreDir);

  const admission = new Admission({
    meshkoreDir: opts.meshkoreDir,
    onEvent: (ev) => {
      broadcast(ev);
      // Also append to timeline so the daily log captures it
      try { appendTimelineEvent(opts.meshkoreDir, ev); } catch {}
    },
  });

  // Initial state load
  try {
    await state.rebuild();
  } catch (err) {
    log.warn('initial state build failed (will retry on first request)', { err: String(err) });
  }

  state.startWatcher();

  // ─── HTTP server ───────────────────────────────────────────────────────

  const server = createServer(async (req, res) => {
    setCors(req, res);
    if (req.method === 'OPTIONS') {
      res.writeHead(204);
      res.end();
      return;
    }
    try {
      await route(req, res, opts, runtime, state, broadcast, admission, workers);
    } catch (err: any) {
      log.error('route error', { url: req.url, err: err?.message });
      sendJson(res, 500, { error: 'internal' });
    }
  });

  // ─── WebSocket server ──────────────────────────────────────────────────

  const wss = new WebSocketServer({ noServer: true });
  server.on('upgrade', (req, socket, head) => {
    if (req.url !== '/events' && !req.url?.startsWith('/events?')) {
      socket.destroy();
      return;
    }
    if (!verifyToken(req, opts.token)) {
      socket.write('HTTP/1.1 401 Unauthorized\r\n\r\n');
      socket.destroy();
      return;
    }
    wss.handleUpgrade(req, socket, head, (client) => {
      wsClients.add(client);
      log.info('ws client connected', { total: wsClients.size });
      client.send(JSON.stringify({ type: 'hello', identity: opts.identity, ts: new Date().toISOString() }));
      client.on('close', () => {
        wsClients.delete(client);
        log.info('ws client disconnected', { total: wsClients.size });
      });
      client.on('error', err => log.warn('ws error', { err: String(err) }));
    });
  });

  // ─── Bind ───────────────────────────────────────────────────────────────

  const bind = opts.bindAddress ?? '127.0.0.1';
  await new Promise<void>((resolve, reject) => {
    server.once('error', reject);
    server.listen(opts.port, bind, () => resolve());
  });
  log.info('daemon listening', { url: `http://${bind}:${opts.port}`, identity: opts.identity });

  return {
    async close() {
      for (const c of wsClients) {
        try { c.close(); } catch {}
      }
      await new Promise<void>((resolve) => server.close(() => resolve()));
    },
    broadcast,
  };
}

// ─── Routing ─────────────────────────────────────────────────────────────

async function route(
  req: IncomingMessage,
  res: ServerResponse,
  opts: ServerOptions,
  runtime: Runtime,
  state: StateManager,
  broadcast: (ev: Record<string, unknown>) => void,
  admission: Admission,
  workers: WorkerPool,
) {
  const url = new URL(req.url ?? '/', 'http://x');
  const p = url.pathname;
  const method = req.method ?? 'GET';

  // Public: health
  if (p === '/health' && method === 'GET') {
    // Include cluster identity so the portal's project switcher can
    // distinguish daemons running on different ports without auth.
    // Also surface the host info so the portal can show "running on
    // <device-name>" next to each worker — the user wanted that
    // visible in the Network view.
    let cluster_id: string | undefined;
    let cluster_name: string | undefined;
    let cluster_type: string | undefined;
    try {
      const s = await state.getState();
      cluster_id = (s.cluster as any)?.id;
      cluster_name = (s.cluster as any)?.name;
      cluster_type = (s.cluster as any)?.type;
    } catch {}
    return sendJson(res, 200, {
      ok: true,
      identity: opts.identity,
      port: opts.port,
      mode: 'server',
      cluster_id,
      cluster_name,
      cluster_type,
      device: deviceInfo(),
      ts: new Date().toISOString(),
    });
  }

  // Authenticated routes from here on
  if (!verifyToken(req, opts.token)) {
    return sendJson(res, 401, { error: 'unauthorized' });
  }

  if (method === 'GET' && p === '/state') {
    const s = await state.getState();
    return sendJson(res, 200, s);
  }
  if (method === 'GET' && p.startsWith('/state/')) {
    const sub = p.slice('/state/'.length);
    const s = await state.getState();
    const part = (s as any)[sub];
    if (part === undefined) return sendJson(res, 404, { error: 'unknown subset' });
    return sendJson(res, 200, part);
  }
  if (method === 'GET' && p === '/reload') {
    const s = await state.rebuild();
    return sendJson(res, 200, { ok: true, generated_at: s.generated_at });
  }
  if (method === 'GET' && p.startsWith('/docs/')) {
    return serveFile(res, path.join(opts.meshkoreDir, 'docs', decodeURIComponent(p.slice('/docs/'.length))));
  }
  if (method === 'GET' && p.startsWith('/modules/')) {
    return serveFile(res, path.join(opts.meshkoreDir, 'modules', decodeURIComponent(p.slice('/modules/'.length))));
  }
  if (method === 'GET' && p.startsWith('/tasks/')) {
    return serveFile(res, path.join(opts.meshkoreDir, 'roadmap', decodeURIComponent(p.slice('/tasks/'.length))));
  }
  if (method === 'GET' && p === '/agents') {
    const agentsDir = path.join(opts.meshkoreDir, 'agents');
    const list: any[] = [];
    if (existsSync(agentsDir)) {
      for (const f of readdirSync(agentsDir)) {
        if (!f.endsWith('.yaml')) continue;
        const id = f.replace(/\.yaml$/, '');
        const local = runtime.readAgentPid(id);
        list.push({ identity: id, pid: local, online: local != null });
      }
    }
    return sendJson(res, 200, list);
  }
  if (method === 'GET' && p === '/credentials') {
    const dir = path.join(opts.meshkoreDir, 'credentials');
    const list: any[] = [];
    if (existsSync(dir)) {
      for (const f of readdirSync(dir)) {
        if (f === 'portal-token') continue;
        const stat = statSync(path.join(dir, f));
        list.push({ name: f, path: `credentials/${f}`, size: stat.size, mtime: stat.mtime });
      }
    }
    return sendJson(res, 200, list);
  }

  if (method === 'POST' && p === '/messages') {
    const body = await readBody(req);
    const msg = JSON.parse(body || '{}');
    if (!msg.text) return sendJson(res, 400, { error: 'text required' });
    const author = msg.author || opts.identity;
    const conv = msg.conv || makeConvSlug(msg.text);
    const ev = appendTimelineEvent(opts.meshkoreDir, {
      type: 'chat.user',
      author,
      conv,
      text: msg.text,
    });
    broadcast(ev);
    return sendJson(res, 201, ev);
  }

  if (method === 'POST' && p === '/tasks') {
    const body = await readBody(req);
    const data = JSON.parse(body || '{}');
    if (!data.title) return sendJson(res, 400, { error: 'title required' });
    if (!data.module) return sendJson(res, 400, { error: 'module required' });
    try {
      const result = await createTask(opts.meshkoreDir, data, opts.identity);
      const ev = appendTimelineEvent(opts.meshkoreDir, {
        type: 'task.created',
        id: result.id,
        title: data.title,
        module: data.module,
        priority: data.priority || 'medium',
        conv: data.conv,
        files: data.files,
      });
      broadcast(ev);
      return sendJson(res, 201, { ...ev, path: result.path });
    } catch (err: any) {
      return sendJson(res, 400, { error: err.message });
    }
  }
  // ─── Dispatch endpoints (V17 — minimal claude-code runner) ────────────
  if (method === 'POST' && /^\/tasks\/[^/]+\/dispatch$/.test(p)) {
    const taskId = p.split('/')[2]!;
    let body: Record<string, unknown> = {};
    try { body = JSON.parse(await readBody(req) || '{}'); } catch {}
    const runnerKind = (body.runner as string) || 'claude-code';
    if (runnerKind !== 'claude-code') {
      return sendJson(res, 400, { error: `runner '${runnerKind}' not implemented (only claude-code right now)` });
    }
    try {
      // Pick the worker that owns the module this task lives under.
      // Falls back to the coordinator if no module-specific worker.
      const tasks = (state.lastBundle as any)?.roadmap?.tasks || [];
      const task = tasks.find((t: any) => t.id === taskId);
      const moduleId: string | null = task?.category || null;
      const workerOverride = body.worker as string | undefined;
      const worker = workerOverride
        ? workers.get(workerOverride)
        : workers.pickForModule(moduleId);
      if (!worker) return sendJson(res, 400, { error: 'no worker available — declare one in the portal Network panel' });

      const runId = `run_${Date.now().toString(36)}_${Math.random().toString(36).slice(2, 6)}`;
      const handle = runClaudeCode({
        meshkoreDir: opts.meshkoreDir,
        taskId,
        identity: worker.id,
        bin: (body.bin as string) || undefined,
        sessionId: worker.session_id,
        model: worker.model,
        permissions: worker.permissions || 'unrestricted',
        emit: (ev) => broadcast(ev),
      });
      workers.touch(worker.id);
      activeRuns.set(taskId, handle);
      // Don't await; let the response return immediately and the portal
      // sees task.* events on the WebSocket as they happen.
      handle.done.then(() => activeRuns.delete(taskId)).catch(() => activeRuns.delete(taskId));
      return sendJson(res, 202, {
        run_id: runId,
        task: taskId,
        runner: runnerKind,
        worker: worker.id,
        model: worker.model,
        identity: (body.identity as string) || opts.identity,
        pid: handle.pid,
        started_at: handle.startedAt,
      });
    } catch (err: any) {
      return sendJson(res, 400, { error: err.message || String(err) });
    }
  }
  if (method === 'POST' && /^\/tasks\/[^/]+\/cancel$/.test(p)) {
    const taskId = p.split('/')[2]!;
    const h = activeRuns.get(taskId);
    if (!h) return sendJson(res, 404, { error: `no active run for ${taskId}` });
    h.cancel();
    return sendJson(res, 202, { task: taskId, cancelled: true });
  }
  if (method === 'POST' && p === '/chat/dispatch') {
    // Coordinator chat: every user query is routed through the master
    // daemon's primary identity. We log the chat.user event, then spawn
    // a claude-code runner with the chat text as the prompt and a short
    // briefing about what it has access to (repo, .meshkore/, the
    // operator manual). No LLM lives in the portal — the agent thinks.
    const body = await readBody(req);
    let data: Record<string, unknown> = {};
    try { data = JSON.parse(body || '{}'); } catch {}
    const text = String(data.text || '').trim();
    if (!text) return sendJson(res, 400, { error: 'text required' });
    const author = String(data.author || opts.identity);
    const conv = String(data.conv || `chat-${new Date().toISOString().slice(0, 16).replace(/[:T]/g, '-').toLowerCase()}`);
    // 1) Append the chat.user event
    const userEv = appendTimelineEvent(opts.meshkoreDir, {
      type: 'chat.user', author, text, conv,
    });
    broadcast(userEv);
    // 2) Spawn the coordinator runner
    const dispatchId = `chat_${Date.now().toString(36)}`;
    try {
      const coord = workers.coordinator();
      const handle = spawnCoordinatorChat({
        meshkoreDir: opts.meshkoreDir,
        identity: coord?.id || opts.identity,
        prompt: text,
        conv,
        state,
        worker: coord,
        emit: (ev) => broadcast(ev),
      });
      if (coord) workers.touch(coord.id);
      activeRuns.set(dispatchId, handle);
      handle.done.then(() => activeRuns.delete(dispatchId)).catch(() => activeRuns.delete(dispatchId));
      return sendJson(res, 202, {
        dispatch_id: dispatchId,
        conv,
        runner: 'claude-code',
        identity: opts.identity,
        pid: handle.pid,
        worker: coord ? {
          id: coord.id,
          model: coord.model,
          session_id: coord.session_id,
          permissions: coord.permissions || 'unrestricted',
        } : null,
      });
    } catch (err: any) {
      return sendJson(res, 400, { error: err.message || String(err) });
    }
  }
  // ─── Version coordinator (V20 — stub) ───────────────────────────────
  // The shape is locked so agents and the portal can call it today;
  // concrete file-locked monotonic counter lands with V20.
  if (method === 'GET' && p === '/version/current') {
    return sendJson(res, 200, {
      current: 'unknown',
      hint: 'V20 — version coordinator not implemented yet. ' +
            'See .meshkore/docs/conventions/versioning.md and ' +
            'modules/daemon/tasks/V20-version-coordinator.md.',
    });
  }
  if (method === 'POST' && p === '/version/next') {
    return sendJson(res, 501, {
      error: 'version coordinator not implemented yet',
      hint: 'V20 — until then, bump versions manually following ' +
            '.meshkore/docs/conventions/versioning.md (SemVer 2.0.0). ' +
            'Coordinate with the human if multiple agents are active.',
    });
  }

  if (method === 'POST' && p === '/agents') {
    // Create an agent identity from the portal wizard. Same logic as
    // `meshcore agent create`, accepts the credential inline.
    const raw = await readBody(req);
    let data: Record<string, unknown> = {};
    try { data = JSON.parse(raw || '{}'); } catch {}
    try {
      const out = createAgentIdentity({
        meshkoreDir: opts.meshkoreDir,
        identity: String(data.identity || ''),
        client:   String(data.client || ''),
        agentRole: data.agent_role ? String(data.agent_role) : undefined,
        credentialValue: data.credential ? String(data.credential) : undefined,
      });
      // Tell the world a new identity was added — portal repaints Network
      const ev = appendTimelineEvent(opts.meshkoreDir, {
        type: 'agent.created',
        identity: data.identity,
        client: data.client,
        agent_role: data.agent_role,
      });
      broadcast(ev);
      // Force a state rebuild so members[] picks up the new agents/<id>.yaml
      state.rebuild().catch(() => {});
      return sendJson(res, 201, {
        ok: true,
        identity: data.identity,
        client: data.client,
        yaml: path.relative(opts.meshkoreDir, out.yamlPath),
        creds: out.credsPath ? path.relative(opts.meshkoreDir, out.credsPath) : null,
      });
    } catch (err: any) {
      return sendJson(res, 400, { error: err.message || String(err) });
    }
  }
  if (method === 'GET' && p === '/runners') {
    const list = Array.from(activeRuns.entries()).map(([taskId, h]) => ({
      identity: opts.identity,
      kind: 'claude-code',
      state: 'busy',
      current_task: taskId,
      started_at: h.startedAt,
      pid: h.pid,
      remote: false,
    }));
    return sendJson(res, 200, { runners: list });
  }

  // ─── Worker pool (persistent sessions) ────────────────────────────────
  if (method === 'GET' && p === '/workers') {
    // Decorate each worker with the host it lives on, so the portal's
    // Network view can label "this worker runs on <device-name>".
    const dev = deviceInfo();
    const list = workers.list().map(w => ({ ...w, device: dev }));
    return sendJson(res, 200, { workers: list, device: dev });
  }
  if (method === 'POST' && p === '/workers') {
    try {
      const data = JSON.parse(await readBody(req) || '{}');
      const w = workers.add({
        id: String(data.id || ''),
        kind: (data.kind || 'claude-code') as any,
        model: String(data.model || 'auto'),
        module: data.module ? String(data.module) : null,
        role: (data.role || 'worker') as any,
        permissions: (data.permissions || 'unrestricted') as any,
        name: data.name ? String(data.name) : undefined,
        notes: data.notes ? String(data.notes) : undefined,
      });
      broadcast({ type: 'worker.added', worker: w, ts: new Date().toISOString() });
      return sendJson(res, 201, w);
    } catch (err: any) {
      return sendJson(res, 400, { error: err.message || String(err) });
    }
  }
  if (method === 'PATCH' && /^\/workers\/[^/]+$/.test(p)) {
    const id = p.split('/')[2]!;
    try {
      const data = JSON.parse(await readBody(req) || '{}');
      const allowed = (({ kind, model, module, role, name, notes, permissions }) => ({ kind, model, module, role, name, notes, permissions }))(data) as any;
      const w = workers.update(id, allowed);
      broadcast({ type: 'worker.updated', worker: w, ts: new Date().toISOString() });
      return sendJson(res, 200, w);
    } catch (err: any) {
      return sendJson(res, 400, { error: err.message || String(err) });
    }
  }
  if (method === 'DELETE' && /^\/workers\/[^/]+$/.test(p)) {
    const id = p.split('/')[2]!;
    try {
      workers.remove(id);
      broadcast({ type: 'worker.removed', id, ts: new Date().toISOString() });
      return sendJson(res, 204, {});
    } catch (err: any) {
      return sendJson(res, 400, { error: err.message || String(err) });
    }
  }
  if (method === 'POST' && /^\/workers\/[^/]+\/reset-session$/.test(p)) {
    const id = p.split('/')[2]!;
    try {
      const w = workers.resetSession(id);
      broadcast({ type: 'worker.session_reset', worker: w, ts: new Date().toISOString() });
      return sendJson(res, 200, w);
    } catch (err: any) {
      return sendJson(res, 400, { error: err.message || String(err) });
    }
  }

  // ─── Admission endpoints ─────────────────────────────────────────────
  if (method === 'GET' && p === '/admission/policy') {
    return sendJson(res, 200, admission.getPolicy());
  }
  if (method === 'GET' && p === '/admission/list') {
    return sendJson(res, 200, {
      members: admission.listMembers(),
      pending: admission.listPending(),
    });
  }
  if (method === 'GET' && p === '/admission/pending') {
    return sendJson(res, 200, admission.listPending());
  }
  if (method === 'GET' && p === '/admission/challenge') {
    return sendJson(res, 200, admission.issueChallenge());
  }
  if (method === 'POST' && p === '/admission/issue-token') {
    const body = await readBody(req);
    const data = JSON.parse(body || '{}');
    if (!data.identity || !data.role) return sendJson(res, 400, { error: 'identity + role required' });
    const t = admission.issueToken({
      identity: data.identity,
      role: data.role,
      agent_role: data.agent_role,
      bound_pubkey_fingerprint: data.bound_pubkey_fingerprint ?? null,
      github_user: data.github_user ?? null,
      issued_by: opts.identity,
      multi_use: data.multi_use ?? false,
      max_uses: data.max_uses ?? 1,
    });
    return sendJson(res, 201, t);
  }
  if (method === 'POST' && p === '/admission/request') {
    const body = await readBody(req);
    let parsed;
    try { parsed = AdmissionRequestSchema.parse(JSON.parse(body || '{}')); }
    catch (err: any) { return sendJson(res, 400, { error: 'bad request', details: err.message }); }
    const result = await admission.processRequest(parsed, 'curl');
    return sendJson(res, result.decision === 'rejected' ? 401 : 200, result);
  }
  if (method === 'POST' && /^\/admission\/approve\/[^/]+$/.test(p)) {
    const id = p.split('/').pop()!;
    const r = admission.approve(id, opts.identity);
    return sendJson(res, r.ok ? 200 : 400, r);
  }
  if (method === 'POST' && /^\/admission\/reject\/[^/]+$/.test(p)) {
    const id = p.split('/').pop()!;
    const body = await readBody(req);
    const data = JSON.parse(body || '{}');
    const r = admission.reject(id, opts.identity, data.reason || '');
    return sendJson(res, r.ok ? 200 : 400, r);
  }
  if (method === 'POST' && /^\/admission\/revoke\/[^/]+$/.test(p)) {
    const id = p.split('/').pop()!;
    const body = await readBody(req);
    const data = JSON.parse(body || '{}');
    const r = admission.revoke(id, opts.identity, data.reason || '');
    return sendJson(res, r.ok ? 200 : 400, r);
  }
  if (method === 'GET' && p.startsWith('/admission/github-keys')) {
    const url = new URL(req.url ?? '/', 'http://x');
    const user = url.searchParams.get('user');
    if (!user) return sendJson(res, 400, { error: 'user query param required' });
    try {
      const { fetchGithubKeys } = await import('./identity.js');
      const keys = await fetchGithubKeys(user);
      return sendJson(res, 200, keys.map(k => ({ algo: k.algo, fingerprint: k.fingerprint, ssh: k.ssh, comment: k.comment })));
    } catch (err: any) {
      return sendJson(res, 502, { error: err.message });
    }
  }

  if (method === 'POST' && /^\/tasks\/[^/]+\/transition$/.test(p)) {
    const id = p.split('/')[2] ?? '';
    if (!id) return sendJson(res, 400, { error: 'task id required' });
    const body = await readBody(req);
    const data = JSON.parse(body || '{}');
    const to = data.to;
    if (!to) return sendJson(res, 400, { error: 'to required' });
    try {
      const result = await transitionTask(opts.meshkoreDir, id, to, data.by || opts.identity);
      const ev = appendTimelineEvent(opts.meshkoreDir, {
        type: 'task.transitioned',
        id,
        from: result.from,
        to,
        by: data.by || opts.identity,
      });
      broadcast(ev);
      return sendJson(res, 200, { ok: true, ...result });
    } catch (err: any) {
      return sendJson(res, 400, { error: err.message });
    }
  }

  return sendJson(res, 404, { error: 'not found', path: p });
}

// ─── Helpers ─────────────────────────────────────────────────────────────

function setCors(req: IncomingMessage, res: ServerResponse) {
  const origin = req.headers.origin;
  let allowed = false;
  if (origin) {
    for (const re of ALLOWED_ORIGINS) {
      if (re.test(origin)) { allowed = true; break; }
    }
  }
  if (allowed && origin) {
    res.setHeader('Access-Control-Allow-Origin', origin);
  } else {
    // Default permissive for null origin (file://) and curl
    res.setHeader('Access-Control-Allow-Origin', '*');
  }
  res.setHeader('Access-Control-Allow-Credentials', 'true');
  res.setHeader('Access-Control-Allow-Headers', 'Authorization, Content-Type');
  res.setHeader('Access-Control-Allow-Methods', 'GET, POST, OPTIONS');
  res.setHeader('Vary', 'Origin');
}

function verifyToken(req: IncomingMessage, expected: string): boolean {
  const auth = req.headers['authorization'];
  if (!auth) {
    // Allow query param ?token=… for WS upgrade
    const url = new URL(req.url ?? '/', 'http://x');
    const qt = url.searchParams.get('token');
    return qt === expected;
  }
  const m = /^Bearer\s+(.+)$/i.exec(String(auth));
  return m?.[1] === expected;
}

function sendJson(res: ServerResponse, status: number, body: unknown) {
  res.writeHead(status, { 'Content-Type': 'application/json; charset=utf-8' });
  res.end(JSON.stringify(body));
}

function serveFile(res: ServerResponse, file: string) {
  if (!existsSync(file)) {
    return sendJson(res, 404, { error: 'not found' });
  }
  const data = readFileSync(file, 'utf8');
  res.writeHead(200, { 'Content-Type': 'text/markdown; charset=utf-8' });
  res.end(data);
}

async function readBody(req: IncomingMessage): Promise<string> {
  return new Promise((resolve, reject) => {
    const chunks: Buffer[] = [];
    req.on('data', c => chunks.push(Buffer.from(c)));
    req.on('end', () => resolve(Buffer.concat(chunks).toString('utf8')));
    req.on('error', reject);
  });
}

function appendTimelineEvent(meshkoreDir: string, event: Record<string, unknown>): Record<string, unknown> {
  const ts = new Date().toISOString();
  const today = ts.slice(0, 10);
  const file = path.join(meshkoreDir, 'timeline', `${today}.jsonl`);
  mkdirSync(path.dirname(file), { recursive: true });
  const enriched = { ts, ...event };
  appendFileSync(file, JSON.stringify(enriched) + '\n');
  return enriched;
}

function makeConvSlug(text: string): string {
  const ts = new Date().toISOString().slice(0, 16).replace(/[:T]/g, '-').toLowerCase();
  const slug = text.toLowerCase()
    .replace(/[^a-z0-9\s-]/g, '')
    .trim()
    .split(/\s+/)
    .slice(0, 4)
    .join('-')
    .slice(0, 30);
  return `${ts}-${slug || 'msg'}`;
}

/**
 * Spawn a coordinator chat session. Currently shares the same runner as
 * task dispatch (claude-code headless), but with a different prompt
 * shape: the chat text becomes the user turn, and we hand the agent
 * the cluster's operator URL + the local repo path so it can read
 * tasks/docs and modify them. Output streams as `chat.assistant`
 * progress events tagged with the same conv id.
 */
function spawnCoordinatorChat(opts: {
  meshkoreDir: string;
  identity: string;
  prompt: string;
  conv: string;
  state: StateManager;
  worker?: WorkerSpec;            // when set, uses --session-id + --model
  emit: (ev: Record<string, unknown>) => void;
}): RunHandle {
  const repoRoot = path.dirname(opts.meshkoreDir);
  const startedAt = new Date().toISOString();
  // One stream id per coordinator run — the portal merges every
  // chat.assistant.delta with the same stream_id into one bubble.
  const streamId = `s_${Date.now().toString(36)}_${Math.random().toString(36).slice(2, 6)}`;

  // Pull recent chat history from STATE so the agent has context.
  // Without this, "continúa con las tareas del chat" has nothing to
  // continue — the coordinator process is fresh every dispatch.
  const recentTurns: string[] = [];
  try {
    const cached = opts.state.lastBundle;
    const events: any[] = (cached as any)?.timeline?.recent_events || [];
    const convTurns = events
      .filter(e => e.conv === opts.conv && (e.type === 'chat.user' || e.type === 'chat.assistant.final' || e.type === 'chat.assistant'))
      .slice(-12);
    for (const t of convTurns) {
      const who = t.type === 'chat.user' ? 'USER' : 'YOU (last turn)';
      const text = String(t.text || '').slice(0, 800);
      if (text) recentTurns.push(`${who}: ${text}`);
    }
  } catch { /* state may be empty on first boot */ }

  const briefing = [
    `You are the coordinator agent for a MeshKore cluster at ${repoRoot}.`,
    `Identity: ${opts.identity}. Conversation id: ${opts.conv}.`,
    ``,
    `Read these before deciding what to do (in order, only what you need):`,
    `  • https://meshkore.com/cluster/operate — operator manual`,
    `  • .meshkore/docs/conventions/versioning.md — commits + versions`,
    `  • .meshkore/docs/conventions/context-workflow.md — every-change checklist`,
    `  • .meshkore/modules/<module>/{README.md,tasks/,log/} — per-module work`,
    ``,
    `Hard rules:`,
    `  • Don't push to git unless the user explicitly asks.`,
    `  • Don't invent version numbers; ask POST localhost:5570/version/next.`,
    `  • Never edit .meshkore/credentials/, .meshkore/.runtime/ or generated state.json.`,
    `  • Reply concisely. The portal renders your stdout as the chat answer.`,
    ``,
    recentTurns.length ? `Recent turns in this conversation:\n${recentTurns.join('\n')}\n` : '',
    `User just said:`,
    opts.prompt,
    ``,
    `If the user is vague (e.g. "continue", "siguiente tarea", "next"), look at the roadmap (state.json or .meshkore/modules/*/tasks/) and pick the highest-priority next/in_progress task that is unblocked. Tell them what you're picking and why before doing the work.`,
  ].filter(s => s !== '').join('\n');

  const args: string[] = ['-p'];
  if (opts.worker?.session_id) args.push('--session-id', opts.worker.session_id);
  if (opts.worker?.model && opts.worker.model !== 'auto') args.push('--model', opts.worker.model);
  const perm = opts.worker?.permissions || 'unrestricted';
  const permMode = perm === 'edits' ? 'acceptEdits'
                 : perm === 'safe'  ? null
                 :                    'bypassPermissions';
  if (permMode) args.push('--permission-mode', permMode);
  args.push(briefing);
  const credEnv = {
    ...loadCredEnv(opts.meshkoreDir, opts.worker?.kind || 'claude-code'),
    ...loadCredEnv(opts.meshkoreDir, opts.identity),
  };
  const child = spawn('claude', args, {
    cwd: repoRoot,
    env: { ...process.env, ...credEnv, MESHKORE_IDENTITY: opts.identity, MESHKORE_CONV: opts.conv },
    stdio: ['ignore', 'pipe', 'pipe'],
  });
  opts.emit({
    type: 'task.started',
    id: `chat:${opts.conv}`,
    agent: opts.identity,
    ts: startedAt,
    runner: 'claude-code',
    conv: opts.conv,
    stream_id: streamId,
  });
  // Open an empty assistant bubble immediately so the user sees something.
  opts.emit({
    type: 'chat.assistant.delta',
    author: opts.identity,
    conv: opts.conv,
    stream_id: streamId,
    text: '',
    ts: startedAt,
  });

  let buf = '';
  let stderrTail = '';
  let lastEmittedLen = 0;
  let throttle: ReturnType<typeof setTimeout> | null = null;

  const flushDelta = () => {
    if (buf.length === lastEmittedLen) return;
    lastEmittedLen = buf.length;
    // Cap text we ship to the portal; the portal will request the full
    // text from /timeline if it ever needs it.
    opts.emit({
      type: 'chat.assistant.delta',
      author: opts.identity,
      conv: opts.conv,
      stream_id: streamId,
      text: buf.slice(0, 16000),
      ts: new Date().toISOString(),
    });
    throttle = null;
  };

  child.stdout?.on('data', (c: Buffer) => {
    buf += c.toString('utf8');
    // Throttle to ~5 fps so we stream live without flooding the WS.
    if (!throttle) throttle = setTimeout(flushDelta, 200);
  });
  child.stderr?.on('data', (c: Buffer) => { stderrTail = (stderrTail + c.toString('utf8')).slice(-2000); });

  const startMs = Date.now();
  const done = new Promise<{ exitCode: number; durationMs: number }>((resolve) => {
    child.on('exit', (code, signal) => {
      // Final flush + final event with the complete text.
      if (throttle) { clearTimeout(throttle); throttle = null; }
      const exitCode = code ?? (signal ? 130 : 1);
      const durationMs = Date.now() - startMs;
      const fullText = buf.trim();
      if (exitCode === 0) {
        // Persist the assistant turn so future dispatches in this conv
        // can read it from state.timeline.recent_events.
        try {
          const persisted = appendTimelineEvent(opts.meshkoreDir, {
            type: 'chat.assistant.final',
            author: opts.identity,
            conv: opts.conv,
            stream_id: streamId,
            text: fullText.slice(0, 16000),
            duration_ms: durationMs,
          });
          opts.emit(persisted);
        } catch { /* fall through */ }
        opts.emit({
          type: 'task.completed',
          id: `chat:${opts.conv}`,
          agent: opts.identity,
          ts: new Date().toISOString(),
          stream_id: streamId,
          summary: fullText.split(/\r?\n/).filter(Boolean).slice(-1)[0]?.slice(0, 300) || 'chat done',
          conv: opts.conv,
        });
      } else {
        opts.emit({
          type: 'task.failed',
          id: `chat:${opts.conv}`,
          agent: opts.identity,
          ts: new Date().toISOString(),
          stream_id: streamId,
          exit_code: exitCode,
          error: stderrTail.slice(-300) || `exit ${exitCode}`,
          conv: opts.conv,
        });
      }
      resolve({ exitCode, durationMs });
    });
    child.on('error', (err) => {
      opts.emit({
        type: 'task.failed',
        id: `chat:${opts.conv}`,
        agent: opts.identity,
        ts: new Date().toISOString(),
        stream_id: streamId,
        error: `spawn claude: ${err.message}`,
        conv: opts.conv,
      });
      resolve({ exitCode: 127, durationMs: Date.now() - startMs });
    });
  });
  return {
    taskId: `chat:${opts.conv}`,
    pid: child.pid ?? -1,
    startedAt,
    cancel() { try { child.kill('SIGTERM'); } catch { /* noop */ } },
    done,
  };
}

async function createTask(meshkoreDir: string, data: Record<string, any>, defaultOwner: string): Promise<{ id: string; path: string }> {
  const moduleId = String(data.module).replace(/[^a-z0-9_-]/gi, '-').toLowerCase();
  const today = new Date().toISOString().slice(0, 10);
  const tasksDir = path.join(meshkoreDir, 'roadmap', 'tasks', moduleId);
  mkdirSync(tasksDir, { recursive: true });

  // Pick next available ID. Prefer T<n>, scan existing tasks/log for the highest number.
  const id = data.id || pickNextId(meshkoreDir, data.id_prefix || 'T');

  const slug = String(data.title).toLowerCase()
    .replace(/[^a-z0-9\s-]/g, '')
    .trim()
    .split(/\s+/)
    .slice(0, 6)
    .join('-')
    .slice(0, 50) || 'task';

  const filePath = path.join(tasksDir, `${id}-${slug}.md`);
  if (existsSync(filePath)) throw new Error(`file already exists: ${filePath}`);

  const fm = [
    '---',
    `id: ${id}`,
    `title: "${(data.title as string).replace(/"/g, "'")}"`,
    `status: ${data.status || 'next'}`,
    `priority: ${data.priority || 'medium'}`,
    `owner: ${data.owner || defaultOwner}`,
    `category: ${moduleId}`,
    `created: ${today}`,
    `updated: ${today}`,
    `tags: ${JSON.stringify(data.tags || [])}`,
    ...(data.depends_on ? [`depends_on: ${JSON.stringify(data.depends_on)}`] : []),
    ...(data.effort ? [`effort: "${data.effort}"`] : []),
    ...(data.files ? [`files: ${JSON.stringify(data.files)}`] : []),
    '---',
    '',
    `# ${id} — ${data.title}`,
    '',
    data.description || data.body || '_No description provided._',
    '',
  ].join('\n');

  writeFileSync(filePath, fm);
  return { id, path: path.relative(path.join(meshkoreDir, 'roadmap'), filePath) };
}

function pickNextId(meshkoreDir: string, prefix: string): string {
  const roadmapDir = path.join(meshkoreDir, 'roadmap');
  let max = 0;
  const re = new RegExp(`^${prefix}(\\d+)`);
  const scan = (dir: string) => {
    if (!existsSync(dir)) return;
    for (const entry of readdirSync(dir, { withFileTypes: true })) {
      if (entry.isDirectory()) scan(path.join(dir, entry.name));
      else if (entry.isFile() && entry.name.endsWith('.md')) {
        const m = re.exec(entry.name);
        if (m && m[1]) {
          const n = parseInt(m[1], 10);
          if (n > max) max = n;
        }
      }
    }
  };
  scan(roadmapDir);
  return `${prefix}${max + 1}`;
}

async function transitionTask(meshkoreDir: string, id: string, to: string, _by: string): Promise<{ from: string; path: string }> {
  const valid = ['backlog', 'next', 'in_progress', 'blocked', 'done', 'cancelled'];
  if (!valid.includes(to)) throw new Error(`invalid status: ${to}`);

  // Find the task .md file by id
  const roadmapDir = path.join(meshkoreDir, 'roadmap');
  const found = findTaskFile(roadmapDir, id);
  if (!found) throw new Error(`task not found: ${id}`);

  const text = readFileSync(found, 'utf8');
  const fmMatch = /^---\s*\n([\s\S]*?)\n---\s*\n/.exec(text);
  if (!fmMatch || !fmMatch[1]) throw new Error(`task has no frontmatter: ${found}`);

  const fm: string = fmMatch[1];
  const fromMatch = /^status:\s*(\S+)/m.exec(fm);
  const from = fromMatch?.[1] ?? 'backlog';

  const newFm = fm
    .replace(/^status:.*$/m, `status: ${to}`)
    .replace(/^updated:.*$/m, `updated: ${new Date().toISOString().slice(0, 10)}`);
  const newText = `---\n${newFm}\n---\n${text.slice(fmMatch[0].length)}`;
  writeFileSync(found, newText);

  return { from, path: path.relative(roadmapDir, found) };
}

function findTaskFile(dir: string, id: string): string | null {
  const entries = readdirSync(dir, { withFileTypes: true });
  for (const e of entries) {
    const full = path.join(dir, e.name);
    if (e.isDirectory()) {
      const found = findTaskFile(full, id);
      if (found) return found;
    } else if (e.isFile() && e.name.endsWith('.md')) {
      const text = readFileSync(full, 'utf8');
      if (new RegExp(`^id:\\s*${escapeRegex(id)}\\s*$`, 'm').test(text)) {
        return full;
      }
    }
  }
  return null;
}

function escapeRegex(s: string): string {
  return s.replace(/[.*+?^${}()|[\]\\]/g, '\\$&');
}
