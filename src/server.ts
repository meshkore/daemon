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

const ALLOWED_ORIGINS = [
  /^https:\/\/portal\.meshkore\.com$/,
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
      await route(req, res, opts, runtime, state, broadcast, admission);
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
) {
  const url = new URL(req.url ?? '/', 'http://x');
  const p = url.pathname;
  const method = req.method ?? 'GET';

  // Public: health
  if (p === '/health' && method === 'GET') {
    return sendJson(res, 200, {
      ok: true,
      identity: opts.identity,
      port: opts.port,
      mode: 'server',
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

async function transitionTask(meshkoreDir: string, id: string, to: string, by: string): Promise<{ from: string; path: string }> {
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
