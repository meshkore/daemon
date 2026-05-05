/**
 * Port lock detection — single-server-mode coordination.
 *
 * The first daemon on a machine to call acquirePort() succeeds and runs
 * the HTTP+WS API. Subsequent calls find the port busy and return
 * { acquired: false, heldBy: <identity-of-server> } so the caller can
 * fall back to agent-only mode.
 *
 * Detection is just bind() failing. No mDNS, no service discovery, no
 * leader election. The server-mode daemon writes its identity to
 * .meshkore/.runtime/server.lock so callers can identify it.
 */
import net from 'node:net';

export interface PortAcquireResult {
  acquired: boolean;
  port: number;
  heldBy?: string;  // identity of the existing server-mode daemon, if known
}

export async function acquirePort(port: number): Promise<PortAcquireResult> {
  return new Promise((resolve) => {
    const server = net.createServer();
    server.once('error', (err: NodeJS.ErrnoException) => {
      if (err.code === 'EADDRINUSE') {
        // TODO: read .meshkore/.runtime/server.lock to learn heldBy
        resolve({ acquired: false, port });
      } else {
        resolve({ acquired: false, port });
      }
    });
    server.once('listening', () => {
      server.close(() => resolve({ acquired: true, port }));
    });
    server.listen(port, '127.0.0.1');
  });
}
