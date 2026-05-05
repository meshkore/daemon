/**
 * `meshcore start` — boot the daemon for one identity.
 *
 * Steps:
 *  1. Read .meshkore/agents/<identity>.yaml; ensure credentials exist.
 *  2. Try to bind localhost:5570 (or cluster.yaml's portal.port).
 *     - Success → server-mode: spin up HTTP API, WS server, cluster
 *       transport.
 *     - Bind failure (port busy) → agent-only mode: skip server, just
 *       run the agent loop and publish events to the cluster channel.
 *  3. Connect to cluster transport (WebSocket to hub, with P2P upgrade
 *     attempt).
 *  4. Enter the agent loop: receive task assignments, run the LLM
 *     client headless, commit/push, publish events.
 *
 * On --detach: fork a background process, write PID to
 * .meshkore/.runtime/agents/<identity>.pid, return.
 */
import { acquirePort } from '../cluster/port-lock.js';
import { loadCluster } from '../state/cluster.js';
import { loadAgent } from '../state/agents.js';

interface StartOptions {
  identity?: string;
  detach: boolean;
  yolo: boolean;
}

export async function startCmd(opts: StartOptions): Promise<void> {
  const cluster = await loadCluster('.meshkore');
  const identity = opts.identity ?? cluster.defaultIdentity;
  if (!identity) {
    throw new Error('No --identity given and no default in cluster.yaml.');
  }

  const agent = await loadAgent('.meshkore', identity);
  console.log(`Starting daemon for ${identity} (client: ${agent.client})…`);

  const port = cluster.portalPort ?? 5570;
  const portResult = await acquirePort(port);

  if (portResult.acquired) {
    console.log(`Bound localhost:${port} — running in SERVER mode`);
    // TODO: start HTTP+WS server (src/server/api.ts + events.ts)
  } else {
    console.log(`Port ${port} busy (server: ${portResult.heldBy}) — running in AGENT-ONLY mode`);
  }

  // TODO: connect to cluster transport, enter agent loop
  console.log('TODO: implementation pending — see task C1 in roadmap.');
}
