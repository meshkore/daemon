/**
 * Runner abstraction (V17 stub).
 *
 * A runner spawns an AI client (claude-code, cursor-cli, codex, qwen,
 * or a custom shell command) headlessly, feeds it a task prompt, and
 * streams the resulting events back to the daemon's event bus.
 *
 * This file is intentionally minimal: the master daemon already
 * exposes the dispatch endpoints (currently returning 501); the actual
 * implementations land here as V17 lands.
 *
 * See .meshkore/modules/daemon/tasks/V17-master-orchestrator.md.
 */

export type RunnerKind = 'claude-code' | 'cursor-cli' | 'codex' | 'qwen' | 'custom';

export interface RunnerStatus {
  identity: string;       // agent identity owning the runner
  kind: RunnerKind;       // which client
  state: 'idle' | 'busy' | 'failed';
  current_task?: string;  // task id while busy
  started_at?: string;    // ISO when current task started
  pid?: number;           // local process when applicable
  remote?: boolean;       // true if the runner lives on a follower daemon
}

export interface DispatchRequest {
  taskId: string;
  identity?: string;        // explicit runner; otherwise master picks one
  prompt?: string;          // override the auto-built prompt
  prefer_remote?: boolean;  // route to a peer daemon if possible
}

export interface RunnerEvent {
  type: 'task.started' | 'task.progress' | 'task.completed' | 'task.failed' | 'commit.pushed';
  taskId: string;
  identity: string;
  ts: string;
  // Free-form payload depending on event type — see timeline event schema.
  [k: string]: unknown;
}

/**
 * Public registry interface. Concrete implementation arrives with V17.
 */
export interface RunnerRegistry {
  list(): RunnerStatus[];
  dispatch(req: DispatchRequest): Promise<{ runner: RunnerStatus; runId: string }>;
  cancel(taskId: string): Promise<boolean>;
}

export class NotImplementedRegistry implements RunnerRegistry {
  list(): RunnerStatus[] { return []; }
  async dispatch(_req: DispatchRequest): Promise<{ runner: RunnerStatus; runId: string }> {
    throw new Error('dispatcher not implemented (V17)');
  }
  async cancel(_taskId: string): Promise<boolean> {
    throw new Error('cancel needs the dispatcher (V17)');
  }
}
