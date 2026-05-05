/**
 * Load .meshkore/agents/<identity>.yaml — per-machine agent declaration.
 */
import { readFileSync } from 'node:fs';
import path from 'node:path';
import YAML from 'yaml';
import { z } from 'zod';

const AgentSchema = z.object({
  identity: z.string(),
  role: z.enum(['owner', 'agent']).default('agent'),
  client: z.enum(['claude-code', 'deepseek', 'qwen', 'cursor', 'custom']),
  agent_role: z.string().default('developer'),
  credentials: z.string().optional(),
  permissions: z.object({
    edit: z.boolean().default(true),
    shell: z.boolean().default(true),
    network: z.enum(['open', 'restricted', 'none']).default('restricted'),
  }).optional(),
});

export type AgentConfig = z.infer<typeof AgentSchema>;

export async function loadAgent(meshkoreDir: string, identity: string): Promise<AgentConfig> {
  const file = path.join(meshkoreDir, 'agents', `${identity}.yaml`);
  const raw = readFileSync(file, 'utf8');
  const parsed = YAML.parse(raw);
  return AgentSchema.parse(parsed);
}
