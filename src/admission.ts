/**
 * Cluster admission module — A4 wire protocol + A5 file-backed state.
 *
 * Three approval flows (all converging at "pubkey added to
 * cluster.yaml.members[]"):
 *
 *   manual           operator clicks "approve" in Manage tab → write member
 *   auto-on-token    one-shot token (1h, single-use, bound to pubkey) → write member
 *   auto-on-github   joiner's pubkey is in github_users[user].keys → write member
 *
 * State machine for an admission request:
 *
 *   pending → approved                  (operator approves, or auto-flow matches)
 *   pending → rejected                  (operator rejects)
 *   pending → expired                   (TTL elapsed without action)
 *
 * Persistence (under .meshkore/.runtime/admission/):
 *
 *   pending/<request_id>.json           one file per pending request
 *   tokens/<token>.json                  one file per outstanding admission token
 *   github-keys-cache/<user>.json       24h cache (managed by identity.ts)
 *
 * Approved members go to .meshkore/public/cluster.yaml `members[]`
 * (committed). The admission module rewrites the YAML file, preserving
 * comments where possible.
 */
import {
  existsSync,
  mkdirSync,
  readFileSync,
  readdirSync,
  rmSync,
  writeFileSync,
} from 'node:fs';
import path from 'node:path';
import YAML from 'yaml';
import { z } from 'zod';

import {
  ParsedSshPubkey,
  parseSshEd25519Pubkey,
  fetchGithubKeys,
  generateAdmissionToken,
  admissionChallenge,
  sshKeygenVerify,
} from './identity.js';
import { log } from './lib/log.js';

// ─── Wire types ─────────────────────────────────────────────────────────

export const AdmissionRequestSchema = z.object({
  identity: z.string().min(3).max(40),
  role: z.enum(['coordinator', 'participant', 'observer']).default('participant'),
  agent_role: z.string().optional(),
  pubkey: z.string(),                                  // ssh-ed25519 wire format
  github_user: z.string().optional(),
  admission_token: z.string().optional(),
  challenge_signature: z.string(),                     // SSH signature blob (ssh-keygen -Y sign output)
  challenge_nonce: z.string(),                          // server-issued nonce echoed back
});
export type AdmissionRequest = z.infer<typeof AdmissionRequestSchema>;

export const PendingRequestSchema = z.object({
  request_id: z.string(),
  ts: z.string(),
  status: z.enum(['pending', 'approved', 'rejected', 'expired']).default('pending'),
  identity_proposal: z.string(),
  role: z.string(),
  agent_role: z.string().optional(),
  pubkey: z.string(),
  pubkey_fingerprint: z.string(),
  github_user: z.string().optional(),
  admission_token_used: z.string().nullable().default(null),
  expires_at: z.string(),
  via: z.enum(['ws-channel', 'localhost', 'curl']).default('localhost'),
});
export type PendingRequest = z.infer<typeof PendingRequestSchema>;

export const AdmissionTokenSchema = z.object({
  token: z.string(),
  identity: z.string(),
  role: z.string(),
  agent_role: z.string().optional(),
  bound_pubkey_fingerprint: z.string().nullable().default(null),
  github_user: z.string().nullable().default(null),
  multi_use: z.boolean().default(false),
  max_uses: z.number().int().positive().default(1),
  uses: z.number().int().min(0).default(0),
  issued_at: z.string(),
  issued_by: z.string(),
  expires_at: z.string(),
});
export type AdmissionToken = z.infer<typeof AdmissionTokenSchema>;

// ─── Module ─────────────────────────────────────────────────────────────

export interface AdmissionDeps {
  meshkoreDir: string;
  /** Called whenever a member is approved/revoked or a request changes
   *  state. Server.ts hooks this to emit WS events. */
  onEvent?: (event: Record<string, unknown>) => void;
}

export class Admission {
  constructor(private deps: AdmissionDeps) {
    this.ensureDirs();
  }

  // ─── Path helpers ─────────────────────────────────────────────────────

  private get runtimeDir() { return path.join(this.deps.meshkoreDir, '.runtime', 'admission'); }
  private get pendingDir() { return path.join(this.runtimeDir, 'pending'); }
  private get tokensDir()  { return path.join(this.runtimeDir, 'tokens'); }
  private get clusterYamlPath() { return path.join(this.deps.meshkoreDir, 'public', 'cluster.yaml'); }

  private ensureDirs() {
    mkdirSync(this.pendingDir, { recursive: true });
    mkdirSync(this.tokensDir,  { recursive: true });
  }

  // ─── Cluster.yaml read/write ──────────────────────────────────────────

  private readCluster(): any {
    if (!existsSync(this.clusterYamlPath)) throw new Error('cluster.yaml not found');
    return YAML.parse(readFileSync(this.clusterYamlPath, 'utf8')) ?? {};
  }

  private writeCluster(cluster: any): void {
    // Preserve key order via YAML.stringify with stable ordering. We
    // accept losing user comments for now (yaml lib limitation). A
    // future improvement: parseDocument + targeted edits.
    const out = YAML.stringify(cluster, { indent: 2, lineWidth: 100 });
    writeFileSync(this.clusterYamlPath, out);
  }

  /** Get the cluster's admission policy (with defaults). */
  getPolicy(): {
    mode: string;
    approval: 'manual' | 'auto-on-token' | 'auto-on-github';
    github_users: string[];
    admission_token_lifetime: number;
    max_pending_requests: number;
  } {
    const c = this.readCluster();
    const p = c.admission ?? {};
    return {
      mode: p.mode ?? 'pubkey',
      approval: p.approval ?? 'manual',
      github_users: p.github_users ?? [],
      admission_token_lifetime: p.admission_token_lifetime ?? 3600,
      max_pending_requests: p.max_pending_requests ?? 50,
    };
  }

  /** Current authorized members (from cluster.yaml). */
  listMembers(): any[] {
    const c = this.readCluster();
    return c.members ?? [];
  }

  // ─── Tokens ───────────────────────────────────────────────────────────

  /** Issue a new one-shot admission token. */
  issueToken(opts: {
    identity: string;
    role: string;
    agent_role?: string;
    bound_pubkey_fingerprint?: string | null;
    github_user?: string | null;
    issued_by: string;
    multi_use?: boolean;
    max_uses?: number;
  }): AdmissionToken {
    const policy = this.getPolicy();
    const token = generateAdmissionToken();
    const now = new Date();
    const expires = new Date(now.getTime() + policy.admission_token_lifetime * 1000);
    const t: AdmissionToken = AdmissionTokenSchema.parse({
      token,
      identity: opts.identity,
      role: opts.role,
      agent_role: opts.agent_role,
      bound_pubkey_fingerprint: opts.bound_pubkey_fingerprint ?? null,
      github_user: opts.github_user ?? null,
      multi_use: opts.multi_use ?? false,
      max_uses: opts.max_uses ?? 1,
      uses: 0,
      issued_at: now.toISOString(),
      issued_by: opts.issued_by,
      expires_at: expires.toISOString(),
    });
    writeFileSync(path.join(this.tokensDir, `${token}.json`), JSON.stringify(t, null, 2), { mode: 0o600 });
    this.emit({ type: 'admission.token_issued', token_prefix: token.slice(0, 8), identity: t.identity, lifetime_s: policy.admission_token_lifetime });
    return t;
  }

  private readToken(token: string): AdmissionToken | null {
    const f = path.join(this.tokensDir, `${token}.json`);
    if (!existsSync(f)) return null;
    try { return AdmissionTokenSchema.parse(JSON.parse(readFileSync(f, 'utf8'))); }
    catch { return null; }
  }

  private consumeToken(token: AdmissionToken): void {
    const uses = (token.uses as number) + 1;
    const maxUses = token.max_uses as number;
    token.uses = uses;
    if (!token.multi_use || uses >= maxUses) {
      const f = path.join(this.tokensDir, `${token.token}.json`);
      try { rmSync(f); } catch {}
    } else {
      writeFileSync(path.join(this.tokensDir, `${token.token}.json`), JSON.stringify(token, null, 2), { mode: 0o600 });
    }
  }

  // ─── Pending requests ────────────────────────────────────────────────

  listPending(): PendingRequest[] {
    if (!existsSync(this.pendingDir)) return [];
    const out: PendingRequest[] = [];
    for (const f of readdirSync(this.pendingDir)) {
      if (!f.endsWith('.json')) continue;
      try {
        const p = PendingRequestSchema.parse(JSON.parse(readFileSync(path.join(this.pendingDir, f), 'utf8')));
        // Filter out expired
        if (new Date(p.expires_at).getTime() < Date.now()) {
          this.expireRequest(p.request_id);
          continue;
        }
        if (p.status === 'pending') out.push(p);
      } catch (err) {
        log.warn('bad pending request file', { file: f, err: String(err) });
      }
    }
    return out.sort((a, b) => a.ts.localeCompare(b.ts));
  }

  private writePending(req: PendingRequest): void {
    writeFileSync(path.join(this.pendingDir, `${req.request_id}.json`), JSON.stringify(req, null, 2));
  }

  private removePending(request_id: string): void {
    const f = path.join(this.pendingDir, `${request_id}.json`);
    if (existsSync(f)) try { rmSync(f); } catch {}
  }

  expireRequest(request_id: string): void {
    this.removePending(request_id);
    this.emit({ type: 'admission.expired', request_id });
  }

  // ─── Challenge issuance ───────────────────────────────────────────────

  /** Issue a challenge nonce for a joiner. The joiner signs
   *  `admissionChallenge(clusterId, nonce)` with their SSH key and
   *  posts back via /admission/request. */
  issueChallenge(): { nonce: string; payload: string; cluster_id: string } {
    const c = this.readCluster();
    const cluster_id = c.id;
    const nonce = generateAdmissionToken().slice(0, 32);
    const payload = admissionChallenge(cluster_id, nonce);
    return { nonce, payload, cluster_id };
  }

  // ─── Core flow ────────────────────────────────────────────────────────

  /**
   * Process an incoming admission request. Decides which flow applies
   * based on the cluster's `admission.approval` policy + what the
   * request carries.
   *
   * Returns { decision, … } where decision is:
   *   'approved'       — member added to cluster.yaml immediately
   *   'pending'        — parked for operator review
   *   'rejected'       — sync-rejected (e.g. signature failed)
   */
  async processRequest(
    req: AdmissionRequest,
    via: 'ws-channel' | 'localhost' | 'curl' = 'localhost',
  ): Promise<{
    decision: 'approved' | 'pending' | 'rejected';
    reason?: string;
    request_id?: string;
    member?: any;
  }> {
    const policy = this.getPolicy();

    // 1. Validate pubkey shape
    let parsed: ParsedSshPubkey;
    try { parsed = parseSshEd25519Pubkey(req.pubkey); }
    catch (e: any) { return { decision: 'rejected', reason: `bad pubkey: ${e.message}` }; }

    // 2. Verify the challenge signature
    const c = this.readCluster();
    const expectedPayload = admissionChallenge(c.id, req.challenge_nonce);
    const sigOk = await sshKeygenVerify({
      payload: expectedPayload,
      signatureArmored: req.challenge_signature,
      pubkey: parsed.ssh,
    });
    if (!sigOk) return { decision: 'rejected', reason: 'signature verification failed' };

    // 3. Check cluster's mode
    if (policy.mode !== 'pubkey') {
      return { decision: 'rejected', reason: `cluster admission mode is ${policy.mode}, not pubkey` };
    }

    // 4. Idempotency: if this pubkey is already a (non-revoked) member, succeed silently
    const existingMembers = (c.members ?? []) as any[];
    const dup = existingMembers.find(m => m.pubkey_fingerprint === parsed.fingerprint && !m.revoked_at);
    if (dup) {
      return { decision: 'approved', member: dup };
    }

    // 5. Try the auto-flows first
    if (req.admission_token) {
      const t = this.readToken(req.admission_token);
      if (!t) return { decision: 'rejected', reason: 'admission token not found or expired' };
      if (new Date(t.expires_at as string).getTime() < Date.now()) {
        try { rmSync(path.join(this.tokensDir, `${t.token}.json`)); } catch {}
        return { decision: 'rejected', reason: 'admission token expired' };
      }
      if (t.bound_pubkey_fingerprint && t.bound_pubkey_fingerprint !== parsed.fingerprint) {
        return { decision: 'rejected', reason: 'pubkey does not match the one bound to this token' };
      }
      // Flow 2: auto-on-token
      const member = this.addMember({
        id: t.identity,
        role: (t.role as any),
        agent_role: t.agent_role,
        pubkey: parsed.ssh,
        pubkey_fingerprint: parsed.fingerprint,
        github_user: t.github_user ?? req.github_user ?? undefined,
        authorized_at: new Date().toISOString().slice(0, 10),
        authorized_by: t.issued_by,
      });
      this.consumeToken(t);
      this.emit({ type: 'admission.auto_approved', via: 'token', member_id: member.id, fingerprint: parsed.fingerprint });
      return { decision: 'approved', member };
    }

    if (policy.approval === 'auto-on-github' && req.github_user) {
      if (!policy.github_users.includes(req.github_user)) {
        return { decision: 'rejected', reason: `github user '${req.github_user}' not in cluster's github_users allowlist` };
      }
      // Verify the pubkey is actually present at github.com/<user>.keys
      let ghKeys: ParsedSshPubkey[];
      try { ghKeys = await fetchGithubKeys(req.github_user); }
      catch (e: any) { return { decision: 'rejected', reason: `github fetch failed: ${e.message}` }; }
      const matched = ghKeys.find(k => k.fingerprint === parsed.fingerprint);
      if (!matched) return { decision: 'rejected', reason: `pubkey not found in github.com/${req.github_user}.keys` };
      const member = this.addMember({
        id: req.identity,
        role: req.role,
        agent_role: req.agent_role,
        pubkey: parsed.ssh,
        pubkey_fingerprint: parsed.fingerprint,
        github_user: req.github_user,
        authorized_at: new Date().toISOString().slice(0, 10),
        authorized_by: `github:${req.github_user}`,
      });
      this.emit({ type: 'admission.auto_approved', via: 'github', member_id: member.id, fingerprint: parsed.fingerprint, github_user: req.github_user });
      return { decision: 'approved', member };
    }

    // 6. Fall back to manual approval (Flow 1)
    if (this.listPending().length >= policy.max_pending_requests) {
      return { decision: 'rejected', reason: 'pending queue full' };
    }
    const request_id = `req_${new Date().toISOString().replace(/[-:T.]/g, '').slice(0, 14)}_${parsed.fingerprint.slice(7, 14)}`;
    const expires_at = new Date(Date.now() + policy.admission_token_lifetime * 1000).toISOString();
    const pending: PendingRequest = {
      request_id,
      ts: new Date().toISOString(),
      status: 'pending',
      identity_proposal: req.identity,
      role: req.role,
      agent_role: req.agent_role,
      pubkey: parsed.ssh,
      pubkey_fingerprint: parsed.fingerprint,
      github_user: req.github_user,
      admission_token_used: req.admission_token ?? null,
      expires_at,
      via,
    };
    this.writePending(pending);
    this.emit({ type: 'admission.requested', request_id, identity_proposal: req.identity, fingerprint: parsed.fingerprint });
    return { decision: 'pending', request_id };
  }

  /** Operator approves a pending request. */
  approve(request_id: string, by: string): { ok: true; member: any } | { ok: false; reason: string } {
    const f = path.join(this.pendingDir, `${request_id}.json`);
    if (!existsSync(f)) return { ok: false, reason: 'request not found' };
    const p = PendingRequestSchema.parse(JSON.parse(readFileSync(f, 'utf8')));
    if (new Date(p.expires_at).getTime() < Date.now()) {
      this.expireRequest(request_id);
      return { ok: false, reason: 'request expired' };
    }
    const member = this.addMember({
      id: p.identity_proposal,
      role: p.role as any,
      agent_role: p.agent_role,
      pubkey: p.pubkey,
      pubkey_fingerprint: p.pubkey_fingerprint,
      github_user: p.github_user,
      authorized_at: new Date().toISOString().slice(0, 10),
      authorized_by: by,
    });
    this.removePending(request_id);
    this.emit({ type: 'admission.approved', request_id, member_id: member.id, by });
    return { ok: true, member };
  }

  reject(request_id: string, by: string, reason: string): { ok: true } | { ok: false; reason: string } {
    if (!existsSync(path.join(this.pendingDir, `${request_id}.json`))) return { ok: false, reason: 'request not found' };
    this.removePending(request_id);
    this.emit({ type: 'admission.rejected', request_id, by, reason });
    return { ok: true };
  }

  revoke(member_id: string, by: string, reason: string): { ok: true } | { ok: false; reason: string } {
    const c = this.readCluster();
    const members = (c.members ?? []) as any[];
    const idx = members.findIndex(m => m.id === member_id && !m.revoked_at);
    if (idx < 0) return { ok: false, reason: 'member not found or already revoked' };
    members[idx].revoked_at = new Date().toISOString().slice(0, 10);
    members[idx].revoked_by = by;
    members[idx].revoke_reason = reason;
    c.members = members;
    this.writeCluster(c);
    this.emit({ type: 'admission.revoked', member_id, by, reason });
    return { ok: true };
  }

  // ─── Member write ─────────────────────────────────────────────────────

  private addMember(member: any): any {
    const c = this.readCluster();
    if (!Array.isArray(c.members)) c.members = [];
    c.members.push(member);
    this.writeCluster(c);
    return member;
  }

  // ─── Helpers ──────────────────────────────────────────────────────────

  private emit(event: Record<string, unknown>): void {
    const enriched = { ts: new Date().toISOString(), ...event };
    this.deps.onEvent?.(enriched);
    log.info('admission event', enriched);
  }
}
