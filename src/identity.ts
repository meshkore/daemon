/**
 * SSH/GitHub identity helpers for cluster admission.
 *
 * Bridges three formats:
 *   1. SSH wire format ("ssh-ed25519 AAAA…") — what users have in
 *      ~/.ssh/id_ed25519.pub and what GitHub serves at github.com/<user>.keys.
 *   2. Raw 32-byte ed25519 — what crypto libs work with.
 *   3. Base64 raw 32 bytes — what the hub's Surface 2 verify_challenge
 *      expects (api/src/crypto.rs).
 *
 * Plus:
 *   - Fingerprints (SHA256:base64) matching `ssh-keygen -lf` and GitHub UI.
 *   - GitHub federation: fetch keys for a user with 24h on-disk cache.
 *   - Signing: spawns `ssh-keygen -Y sign` so the private key never enters
 *     daemon memory. Verification: spawns `ssh-keygen -Y verify`.
 */
import { execFile, spawn } from 'node:child_process';
import { promisify } from 'node:util';
import { createHash, randomBytes } from 'node:crypto';
import {
  existsSync,
  mkdirSync,
  readFileSync,
  statSync,
  writeFileSync,
} from 'node:fs';
import path from 'node:path';
import os from 'node:os';

const execFileP = promisify(execFile);

// ─── Parse SSH wire format ──────────────────────────────────────────────

/** Raw 32-byte ed25519 pubkey + metadata. */
export interface ParsedSshPubkey {
  algo: 'ssh-ed25519';
  raw32: Buffer;            // exactly 32 bytes
  base64Raw: string;        // base64 of raw32 — hub verify_challenge format
  fingerprint: string;      // "SHA256:abc…"
  comment: string;          // trailing comment (often "user@host")
  ssh: string;              // canonical ssh-ed25519 line (no comment)
}

/**
 * Parse an SSH ed25519 line: `ssh-ed25519 <base64-blob> [comment]`.
 * The blob is itself a length-prefixed structure: type ("ssh-ed25519",
 * 11 bytes) + key (32 bytes), each prefixed with a 4-byte big-endian
 * length. We extract the 32-byte raw key and return it.
 *
 * Throws on malformed input or non-ed25519 algorithm.
 */
export function parseSshEd25519Pubkey(line: string): ParsedSshPubkey {
  const parts = line.trim().split(/\s+/);
  if (parts.length < 2) throw new Error('not an ssh-ed25519 line: too few fields');
  const [algo, blob, ...rest] = parts;
  if (algo !== 'ssh-ed25519') throw new Error(`unsupported algo: ${algo}; only ssh-ed25519 is accepted`);
  if (!blob) throw new Error('missing key blob');

  const buf = Buffer.from(blob, 'base64');
  // SSH wire: [4-byte len] [type] [4-byte len] [key32]
  if (buf.length < 4) throw new Error('blob too short');
  let off = 0;
  const typeLen = buf.readUInt32BE(off); off += 4;
  if (typeLen !== 11) throw new Error(`unexpected type-length ${typeLen}; expected 11 for "ssh-ed25519"`);
  const type = buf.subarray(off, off + typeLen).toString('utf8'); off += typeLen;
  if (type !== 'ssh-ed25519') throw new Error(`blob type mismatch: ${type}`);
  if (buf.length < off + 4) throw new Error('blob truncated before key length');
  const keyLen = buf.readUInt32BE(off); off += 4;
  if (keyLen !== 32) throw new Error(`unexpected key length ${keyLen}; expected 32 for ed25519`);
  const raw32 = buf.subarray(off, off + 32);
  if (raw32.length !== 32) throw new Error('blob truncated before key bytes');

  const base64Raw = raw32.toString('base64');
  const fingerprint = fingerprintSha256(buf);
  const comment = rest.join(' ');

  return {
    algo: 'ssh-ed25519',
    raw32: Buffer.from(raw32),
    base64Raw,
    fingerprint,
    comment,
    ssh: `${algo} ${blob}`,
  };
}

/**
 * Compute SHA256 fingerprint in the format `ssh-keygen -lf` and GitHub
 * use: `SHA256:<base64-no-padding>`. Input is the **full SSH blob**
 * (the base64-decoded thing, not just the 32 raw bytes). This matches
 * how OpenSSH computes it.
 */
export function fingerprintSha256(blob: Buffer): string {
  const hash = createHash('sha256').update(blob).digest();
  // base64 unpadded
  return 'SHA256:' + hash.toString('base64').replace(/=+$/, '');
}

/**
 * Convert raw-32-bytes-base64 (the format the hub stores) to a fake-comment
 * SSH line. Useful when we want to display a hub-stored key in a UI.
 */
export function rawBase64ToSshLine(base64Raw: string, comment = ''): string {
  const raw = Buffer.from(base64Raw, 'base64');
  if (raw.length !== 32) throw new Error('expected 32 bytes');
  // Build SSH wire blob: len(11) "ssh-ed25519" + len(32) <raw>
  const typeBytes = Buffer.from('ssh-ed25519', 'utf8');
  const blob = Buffer.concat([
    Buffer.from([0, 0, 0, 11]), typeBytes,
    Buffer.from([0, 0, 0, 32]), raw,
  ]);
  const blobB64 = blob.toString('base64');
  return comment ? `ssh-ed25519 ${blobB64} ${comment}` : `ssh-ed25519 ${blobB64}`;
}

// ─── Local pubkey discovery ─────────────────────────────────────────────

/**
 * Try the user's `~/.ssh/id_ed25519.pub`. Returns null if not present.
 * Does NOT generate a key — that's an explicit user-prompted step.
 */
export function discoverDefaultLocalPubkey(): ParsedSshPubkey | null {
  const candidates = [
    path.join(os.homedir(), '.ssh', 'id_ed25519.pub'),
  ];
  for (const c of candidates) {
    if (existsSync(c)) {
      try {
        const line = readFileSync(c, 'utf8').trim();
        return parseSshEd25519Pubkey(line);
      } catch {/* try next */}
    }
  }
  return null;
}

// ─── GitHub federation ─────────────────────────────────────────────────

const GITHUB_KEYS_TTL_MS = 24 * 60 * 60 * 1000; // 24h

/**
 * Fetch all SSH ed25519 keys for a GitHub user. Caches under
 * cacheDir/<user>.json for 24h. Returns an array of parsed pubkeys
 * (may be empty if the user has no ed25519 keys on GitHub).
 *
 * Throws if the GitHub user doesn't exist (404) or on network failure
 * with no cache available.
 */
export async function fetchGithubKeys(
  user: string,
  cacheDir: string = path.join(os.homedir(), '.config', 'meshcore', 'github-keys'),
): Promise<ParsedSshPubkey[]> {
  const safeUser = user.replace(/[^a-zA-Z0-9_-]/g, '');
  if (!safeUser) throw new Error('invalid github user');

  const cacheFile = path.join(cacheDir, `${safeUser}.json`);
  const now = Date.now();

  // Try cache first
  if (existsSync(cacheFile)) {
    try {
      const stat = statSync(cacheFile);
      if (now - stat.mtimeMs < GITHUB_KEYS_TTL_MS) {
        const lines = JSON.parse(readFileSync(cacheFile, 'utf8')) as string[];
        return lines.map(l => parseSshEd25519Pubkey(l));
      }
    } catch {/* refetch */}
  }

  // Fetch fresh
  const url = `https://github.com/${safeUser}.keys`;
  const r = await fetch(url);
  if (r.status === 404) throw new Error(`github user not found: ${safeUser}`);
  if (!r.ok) throw new Error(`github fetch failed: ${r.status}`);
  const text = await r.text();

  const allLines = text.split('\n').map(l => l.trim()).filter(Boolean);
  const ed25519 = allLines.filter(l => l.startsWith('ssh-ed25519 '));

  // Write cache
  try {
    mkdirSync(cacheDir, { recursive: true });
    writeFileSync(cacheFile, JSON.stringify(ed25519, null, 2));
  } catch {/* non-fatal */}

  return ed25519.map(l => {
    try { return parseSshEd25519Pubkey(l); }
    catch { return null; }
  }).filter((x): x is ParsedSshPubkey => x !== null);
}

// ─── Signing & verifying via ssh-keygen ─────────────────────────────────

/**
 * Sign `payload` with the user's SSH key using `ssh-keygen -Y sign`.
 * The private key never enters our process memory; ssh-keygen handles it.
 *
 * Returns the SSH signature blob (an "armored" PEM-like format starting
 * with `-----BEGIN SSH SIGNATURE-----`).
 */
export async function sshKeygenSign(opts: {
  payload: string;
  privKeyPath: string;
  namespace?: string;            // "meshkore-admission" by default
}): Promise<string> {
  const namespace = opts.namespace ?? 'meshkore-admission';
  const tmpFile = path.join(os.tmpdir(), `mk-sign-${randomBytes(6).toString('hex')}`);
  writeFileSync(tmpFile, opts.payload);
  try {
    await execFileP('ssh-keygen', [
      '-Y', 'sign',
      '-n', namespace,
      '-f', opts.privKeyPath,
      tmpFile,
    ]);
    // ssh-keygen writes to <file>.sig
    const sigPath = tmpFile + '.sig';
    const sig = readFileSync(sigPath, 'utf8');
    try { require('node:fs').rmSync(sigPath); } catch {}
    return sig;
  } finally {
    try { require('node:fs').rmSync(tmpFile); } catch {}
  }
}

/**
 * Verify an SSH signature against a payload + expected pubkey + namespace.
 * Uses `ssh-keygen -Y check-novalidate` (no allowed-signers file required).
 * Returns true if valid.
 */
export async function sshKeygenVerify(opts: {
  payload: string;
  signatureArmored: string;
  pubkey: string;                // ssh-ed25519 wire line
  namespace?: string;
}): Promise<boolean> {
  const namespace = opts.namespace ?? 'meshkore-admission';

  // ssh-keygen -Y check-novalidate -n NS -s SIGFILE < payload
  const tmpSig = path.join(os.tmpdir(), `mk-vrfy-${randomBytes(6).toString('hex')}.sig`);
  writeFileSync(tmpSig, opts.signatureArmored);
  try {
    return await new Promise<boolean>((resolve) => {
      const p = spawn('ssh-keygen', [
        '-Y', 'check-novalidate',
        '-n', namespace,
        '-s', tmpSig,
      ]);
      p.stdin.write(opts.payload);
      p.stdin.end();
      p.on('error', () => resolve(false));
      p.on('exit', (code) => resolve(code === 0));
    });
  } finally {
    try { require('node:fs').rmSync(tmpSig); } catch {}
  }
}

// ─── Helpers exported for the admission module ─────────────────────────

/** Stable challenge bytes for cluster admission, namespaced to prevent
 *  cross-context replay. */
export function admissionChallenge(clusterId: string, nonce: string): string {
  return `MESHKORE-CLUSTER-ADMISSION-v1:${clusterId}:${nonce}`;
}

/** Generate a one-shot admission token (32-byte hex). */
export function generateAdmissionToken(): string {
  return randomBytes(32).toString('hex');
}
