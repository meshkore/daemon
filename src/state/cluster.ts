/**
 * Load and validate .meshkore/public/cluster.yaml.
 */
import { readFileSync } from 'node:fs';
import path from 'node:path';
import YAML from 'yaml';
import { z } from 'zod';

// Admission policy — extends the legacy `.meshkore` v1 spec (5 modes) with
// approval sub-policies (manual / auto-on-token / auto-on-github) within
// the `pubkey` mode. See docs/architecture/admission.md.
const AdmissionSchema = z.object({
  mode: z.enum(['open', 'invite', 'invite+approval', 'allowlist', 'pubkey']).default('pubkey'),
  approval: z.enum(['manual', 'auto-on-token', 'auto-on-github']).default('manual'),
  github_users: z.array(z.string()).default([]),
  admission_token_lifetime: z.number().int().positive().default(3600),
  max_pending_requests: z.number().int().positive().default(50),
}).optional();

// One authorized cluster member (committed to public/cluster.yaml). The
// pubkey is public by definition; the matching private key never leaves
// the member's machine.
const MemberSchema = z.object({
  id: z.string().regex(/^[a-z0-9][a-z0-9-]{2,40}[a-z0-9]$/),
  role: z.enum(['coordinator', 'participant', 'observer']).default('participant'),
  agent_role: z.string().optional(), // functional: developer | reviewer | deployer | tester | …
  pubkey: z.string().optional(),     // SSH ed25519 wire format: "ssh-ed25519 AAAA…"
  pubkey_fingerprint: z.string().optional(), // SHA256:base64 (matches GitHub display)
  github_user: z.string().optional(),
  capabilities: z.array(z.string()).default([]),
  authorized_at: z.string().optional(),
  authorized_by: z.string().optional(),
  revoked_at: z.string().optional(),
  revoked_by: z.string().optional(),
  revoke_reason: z.string().optional(),
});

const ClusterSchema = z.object({
  version: z.literal(1),
  id: z.string(),
  type: z.enum(['dev', 'comms', 'service', 'mixed']),
  name: z.string(),
  description: z.string().optional(),
  transport: z.object({
    protocol: z.enum(['websocket', 'sse', 'nats']).default('websocket'),
    endpoint: z.string().url(),
    fallback: z.string().url().optional(),
  }),
  bootstrap: z.record(z.string()).optional(),
  git: z.object({
    repo: z.string().optional(),
    branch: z.string().default('main'),
    auto_pull: z.boolean().default(true),
    auto_commit: z.boolean().default(false),
  }).optional(),
  portal: z.object({
    port: z.number().default(5570),
  }).optional(),
  profile: z.record(z.unknown()).optional(),
  admission: AdmissionSchema,
  members: z.array(MemberSchema).default([]),
});

export type ClusterMember = z.infer<typeof MemberSchema>;
export type ClusterAdmission = NonNullable<z.infer<typeof AdmissionSchema>>;

export type ClusterConfig = z.infer<typeof ClusterSchema> & {
  defaultIdentity?: string;
  portalPort: number;
};

export async function loadCluster(meshkoreDir: string): Promise<ClusterConfig> {
  const file = path.join(meshkoreDir, 'public', 'cluster.yaml');
  const raw = readFileSync(file, 'utf8');
  const parsed = YAML.parse(raw);
  const validated = ClusterSchema.parse(parsed);
  return {
    ...validated,
    portalPort: validated.portal?.port ?? 5570,
  };
}
