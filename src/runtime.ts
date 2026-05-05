/**
 * .meshkore/.runtime/ — daemon ephemera (PIDs, tokens, locks, logs).
 *
 * server.lock        JSON: {pid, identity, port, started_at}
 * agents/<id>.pid    text: PID number
 * portal-token       text: bearer token (also lives in credentials/)
 */
import {
  existsSync,
  mkdirSync,
  readdirSync,
  readFileSync,
  rmSync,
  writeFileSync,
} from 'node:fs';
import path from 'node:path';
import crypto from 'node:crypto';

export interface ServerLock {
  pid: number;
  identity: string;
  port: number;
  started_at: string;
  token: string;
}

export class Runtime {
  constructor(public meshkoreDir: string) {}

  private get runtimeDir() { return path.join(this.meshkoreDir, '.runtime'); }
  private get serverLockPath() { return path.join(this.runtimeDir, 'server.lock'); }
  private get agentsDir() { return path.join(this.runtimeDir, 'agents'); }
  private get tokenPath() { return path.join(this.meshkoreDir, 'credentials', 'portal-token'); }

  ensure() {
    mkdirSync(this.runtimeDir, { recursive: true });
    mkdirSync(this.agentsDir, { recursive: true });
    mkdirSync(path.join(this.meshkoreDir, 'credentials'), { recursive: true });
  }

  // ─── Server lock ───────────────────────────────────────────────────────

  readServerLock(): ServerLock | null {
    if (!existsSync(this.serverLockPath)) return null;
    try {
      const raw = readFileSync(this.serverLockPath, 'utf8');
      return JSON.parse(raw) as ServerLock;
    } catch { return null; }
  }

  writeServerLock(lock: ServerLock) {
    this.ensure();
    writeFileSync(this.serverLockPath, JSON.stringify(lock, null, 2));
  }

  clearServerLock() {
    if (existsSync(this.serverLockPath)) rmSync(this.serverLockPath);
  }

  isServerAlive(): { alive: boolean; lock?: ServerLock } {
    const lock = this.readServerLock();
    if (!lock) return { alive: false };
    if (!isProcessAlive(lock.pid)) return { alive: false, lock };
    return { alive: true, lock };
  }

  // ─── Agent PIDs ─────────────────────────────────────────────────────────

  writeAgentPid(identity: string, pid: number) {
    this.ensure();
    writeFileSync(path.join(this.agentsDir, `${identity}.pid`), String(pid));
  }

  readAgentPid(identity: string): number | null {
    const p = path.join(this.agentsDir, `${identity}.pid`);
    if (!existsSync(p)) return null;
    return parseInt(readFileSync(p, 'utf8').trim(), 10);
  }

  clearAgentPid(identity: string) {
    const p = path.join(this.agentsDir, `${identity}.pid`);
    if (existsSync(p)) rmSync(p);
  }

  listAgentPids(): { identity: string; pid: number; alive: boolean }[] {
    if (!existsSync(this.agentsDir)) return [];
    const result: { identity: string; pid: number; alive: boolean }[] = [];
    for (const f of readdirSync(this.agentsDir)) {
      if (!f.endsWith('.pid')) continue;
      const identity = f.replace(/\.pid$/, '');
      const pid = this.readAgentPid(identity);
      if (pid != null) result.push({ identity, pid, alive: isProcessAlive(pid) });
    }
    return result;
  }

  // ─── Portal token ───────────────────────────────────────────────────────

  getOrCreateToken(): string {
    if (existsSync(this.tokenPath)) {
      return readFileSync(this.tokenPath, 'utf8').trim();
    }
    this.ensure();
    const token = crypto.randomBytes(24).toString('hex');
    writeFileSync(this.tokenPath, token + '\n', { mode: 0o600 });
    return token;
  }

  readToken(): string | null {
    if (!existsSync(this.tokenPath)) return null;
    return readFileSync(this.tokenPath, 'utf8').trim();
  }
}

function isProcessAlive(pid: number): boolean {
  try {
    process.kill(pid, 0);
    return true;
  } catch (e: any) {
    return e?.code === 'EPERM';
  }
}
