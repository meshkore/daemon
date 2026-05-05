/**
 * `meshcore stop` — stop one or all daemons on this machine.
 *
 * SIGTERM the PID(s), wait briefly, SIGKILL on timeout.
 * Cleans up .meshkore/.runtime/ entries.
 */
interface StopOptions {
  identity?: string;
}

export async function stopCmd(_opts: StopOptions): Promise<void> {
  // TODO: implement
  console.log('TODO: implementation pending — see task C1 in roadmap.');
}
