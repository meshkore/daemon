/**
 * Fetch helpers for the online catalog at meshkore.com/reference/.
 */
import { writeFileSync, mkdirSync } from 'node:fs';
import path from 'node:path';

import { log } from './lib/log.js';

const DEFAULT_BASE = process.env.MESHCORE_CATALOG_BASE || 'https://meshkore.com/reference';

export interface CatalogClient {
  base: string;
  fetchText(rel: string): Promise<string>;
  download(rel: string, destPath: string): Promise<void>;
}

export function makeCatalogClient(base = DEFAULT_BASE): CatalogClient {
  return {
    base,
    async fetchText(rel: string): Promise<string> {
      const url = `${base}/${rel.replace(/^\//, '')}`;
      const r = await fetch(url);
      if (!r.ok) throw new Error(`catalog fetch failed: ${url} → ${r.status}`);
      return r.text();
    },
    async download(rel: string, destPath: string): Promise<void> {
      const url = `${base}/${rel.replace(/^\//, '')}`;
      log.debug('catalog download', { url, dest: destPath });
      const r = await fetch(url);
      if (!r.ok) throw new Error(`catalog download failed: ${url} → ${r.status}`);
      const text = await r.text();
      mkdirSync(path.dirname(destPath), { recursive: true });
      writeFileSync(destPath, text);
    },
  };
}

/** Render a template string with {{placeholder}} substitution. */
export function renderTemplate(text: string, vars: Record<string, string>): string {
  return text.replace(/\{\{(\w+)\}\}/g, (_, k) => vars[k] ?? '');
}
