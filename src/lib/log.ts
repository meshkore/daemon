/**
 * Structured logger.
 * - Default: JSON-line to stderr (machine-readable, easy to grep + tail).
 * - With MESHCORE_LOG=pretty: human-readable color output.
 */
import chalk from 'chalk';

type Level = 'debug' | 'info' | 'warn' | 'error';

const LEVEL_COLOR: Record<Level, (s: string) => string> = {
  debug: chalk.gray,
  info: chalk.cyan,
  warn: chalk.yellow,
  error: chalk.red,
};

const PRETTY = process.env.MESHCORE_LOG === 'pretty' || process.stderr.isTTY;
const VERBOSE = process.env.MESHCORE_VERBOSE === '1';

function emit(level: Level, msg: string, fields?: Record<string, unknown>) {
  if (level === 'debug' && !VERBOSE) return;

  const ts = new Date().toISOString();
  if (PRETTY) {
    const tag = LEVEL_COLOR[level](level.padEnd(5));
    let line = `${chalk.gray(ts.slice(11, 19))} ${tag} ${msg}`;
    if (fields && Object.keys(fields).length) {
      line += ' ' + chalk.gray(JSON.stringify(fields));
    }
    process.stderr.write(line + '\n');
  } else {
    process.stderr.write(JSON.stringify({ ts, level, msg, ...fields }) + '\n');
  }
}

export const log = {
  debug: (msg: string, fields?: Record<string, unknown>) => emit('debug', msg, fields),
  info:  (msg: string, fields?: Record<string, unknown>) => emit('info', msg, fields),
  warn:  (msg: string, fields?: Record<string, unknown>) => emit('warn', msg, fields),
  error: (msg: string, fields?: Record<string, unknown>) => emit('error', msg, fields),
};
