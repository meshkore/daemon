#!/usr/bin/env node
/**
 * meshcore — entry point.
 */
import { Command } from 'commander';
import { initCmd } from './commands/init.js';
import { startCmd } from './commands/start.js';
import { statusCmd } from './commands/status.js';
import { stopCmd } from './commands/stop.js';
import { tasksCmd } from './commands/tasks.js';
import { sendCmd } from './commands/send.js';
import { peekCmd } from './commands/peek.js';
import { logCmd } from './commands/log.js';
import { installHooksCmd } from './commands/hooks.js';
import { agentCmd } from './commands/agent.js';
import { log } from './lib/log.js';

const program = new Command();

program
  .name('meshcore')
  .description('MeshKore cluster daemon')
  .version('0.0.2');

program
  .command('init')
  .description('Initialize .meshkore/ in the current repo')
  .option('--type <type>', 'cluster type: dev | comms | service | mixed', 'dev')
  .option('--id <id>', 'cluster id (defaults to <repo-name>-cluster)')
  .option('--name <name>', 'human-readable cluster name')
  .option('--description <text>', 'one-line description')
  .option('--yes', 'skip interactive prompts (use defaults)', false)
  .action(initCmd);

program
  .command('start')
  .description('Start the daemon for one identity')
  .option('--identity <id>', 'agent identity from .meshkore/agents/')
  .option('--detach', 'run in background', false)
  .option('--port <number>', 'override portal port', (v) => parseInt(v, 10))
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
  .description('List tasks from the local roadmap')
  .option('--status <list>', 'comma-separated status filter', 'next,in_progress,blocked')
  .option('--module <id>', 'filter by module / category')
  .option('--limit <n>', 'max rows', (v) => parseInt(v, 10), 80)
  .action(tasksCmd);

program
  .command('send <text...>')
  .description('Post a chat.user message to the local daemon')
  .option('--conv <id>', 'conversation slug (auto-generated otherwise)')
  .option('--author <id>', 'author identity (defaults to server-mode identity)')
  .action((textArr: string[], opts: { conv?: string; author?: string }) => {
    return sendCmd({ text: textArr.join(' '), ...opts });
  });

program
  .command('peek')
  .description('Stream events from the local daemon WebSocket')
  .action(peekCmd);

program
  .command('log')
  .description('Generate the daily log .md from .meshkore/timeline/')
  .option('--date <YYYY-MM-DD>', 'specific UTC date (default: today)')
  .option('--since <YYYY-MM-DD>', 'start of range')
  .option('--until <YYYY-MM-DD>', 'end of range')
  .action(logCmd);

program
  .command('install-hooks')
  .description('Symlink .meshkore/scripts/git-hooks/* into .git/hooks/ (post-commit auto-logs to timeline)')
  .action(installHooksCmd);

const agent = program.command('agent').description('Manage agent identities');
agent
  .command('create')
  .description('Add a new agent identity to .meshkore/agents/')
  .requiredOption('--client <name>', 'claude-code | deepseek | qwen | cursor | custom')
  .requiredOption('--identity <id>', 'unique identity for this machine')
  .option('--role <role>', 'agent_role: developer | reviewer | deployer | tester', 'developer')
  .option('--no-prompt', 'do not prompt for credentials', false)
  .action(agentCmd.create);
agent
  .command('list')
  .description('List agents declared for this cluster')
  .action(agentCmd.list);

program.parseAsync(process.argv).catch((err) => {
  log.error('command failed', { msg: err?.message ?? String(err) });
  if (process.env.MESHCORE_VERBOSE === '1') console.error(err?.stack);
  process.exit(1);
});
