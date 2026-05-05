/**
 * AgentClient — pluggable interface for any LLM-backed agent.
 *
 * Built-in implementations live alongside this file:
 *   - claude-code.ts (Anthropic Claude Code SDK headless)
 *   - deepseek.ts    (DeepSeek API)
 *   - qwen.ts        (Alibaba Qwen / DashScope)
 *   - cursor.ts      (Cursor CLI)
 *   - custom.ts      (user-provided shell command)
 */
export interface Credentials {
  apiKey?: string;
  envFile?: string; // path to .env-style file
  [k: string]: unknown;
}

export interface TaskInput {
  id: string;
  title: string;
  body: string;
  category: string;
  taskMdPath: string; // path under .meshkore/roadmap/
  repoPath: string;   // absolute path to repo root
}

export interface TaskResult {
  ok: boolean;
  filesChanged: string[];
  summary: string;
  errorMessage?: string;
}

export interface AgentClient {
  /** Display name, e.g. "claude-code" */
  readonly name: string;

  /** One-time setup: validate creds, prepare any cache */
  init(creds: Credentials): Promise<void>;

  /** Run a task headless against the local repo */
  runTask(input: TaskInput): Promise<TaskResult>;

  /** Quick liveness check */
  healthcheck(): Promise<boolean>;
}
