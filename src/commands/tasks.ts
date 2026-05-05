/**
 * `meshcore tasks` — list tasks from .meshkore/roadmap/state.json.
 *
 * If a daemon is running locally, fetch from
 * http://localhost:5570/state/roadmap. Otherwise read state.json
 * directly from disk.
 */
interface TasksOptions {
  status: string; // comma-separated
}

export async function tasksCmd(_opts: TasksOptions): Promise<void> {
  // TODO: implement
  console.log('TODO: implementation pending — see task C1 in roadmap.');
}
