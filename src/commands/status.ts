/**
 * `meshcore status` — show all daemons running on this machine.
 *
 * Reads .meshkore/.runtime/agents/*.pid, checks each PID is alive,
 * fetches localhost:5570/health to identify the server-mode daemon,
 * prints a table.
 */
export async function statusCmd(): Promise<void> {
  // TODO: implement
  console.log('TODO: implementation pending — see task C1 in roadmap.');
}
