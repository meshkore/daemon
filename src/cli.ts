#!/usr/bin/env node
/**
 * meshcore — entry point.
 * Dispatches to one of the command handlers in src/commands/.
 */
import { Command } from 'commander';
import { initCmd } from './commands/init.js';
import { startCmd } from './commands/start.js';
import { statusCmd } from './commands/status.js';
import { stopCmd } from './commands/stop.js';
import { tasksCmd } from './commands/tasks.js';
import { agentCmd } from './commands/agent.js';

const program = new Command();

program
  .name('meshcore')
  .description('MeshKore cluster daemon')
  .version('0.0.1');

program
  .command('init')
  .description('Initialize .meshkore/ in the current repo')
  .option('--type <type>', 'cluster type: dev | comms | service | mixed', 'dev')
  .option('--id <id>', 'cluster id (defaults to <repo-name>-cluster)')
  .option('--name <name>', 'human-readable cluster name')
  .action(initCmd);

program
  .command('start')
  .description('Start the daemon for one identity')
  .option('--identity <id>', 'agent identity from .meshkore/agents/')
  .option('--detach', 'run in background', false)
  .option('--yolo', 'skip confirmation prompts (CI mode)', false)
  .action(startCmd);

program
  .command('status')
  .description('Show status of all daemons running on this machine')
  .action(statusCmd);

program
  .command('stop')
  .description('Stop a daemon (or all)')
  .option('--identity <id>', 'specific identity; without it, stops all')
  .action(stopCmd);

program
  .command('tasks')
  .description('List tasks in the local roadmap')
  .option('--status <status>', 'filter by status', 'next,in_progress')
  .action(tasksCmd);

const agent = program.command('agent').description('Manage agent identities');
agent
  .command('create')
  .description('Add a new agent identity to .meshkore/agents/')
  .option('--client <name>', 'claude-code | deepseek | qwen | cursor | custom')
  .option('--identity <id>', 'unique identity for this machine')
  .option('--role <role>', 'agent_role: developer | reviewer | deployer | …', 'developer')
  .action(agentCmd.create);
agent
  .command('list')
  .description('List agents declared for this cluster')
  .action(agentCmd.list);

program.parseAsync(process.argv).catch((err) => {
  console.error(err.message);
  process.exit(1);
});
