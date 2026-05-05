/**
 * Load and validate .meshkore/public/cluster.yaml.
 */
import { readFileSync } from 'node:fs';
import path from 'node:path';
import YAML from 'yaml';
import { z } from 'zod';

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
});

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
