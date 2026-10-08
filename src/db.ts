import { readFileSync } from 'node:fs';
import pg from 'pg';
import { config } from './config.js';

export const pool = new pg.Pool({ connectionString: config.databaseUrl, max: 4 });

export async function migrate(): Promise<void> {
  const sql = readFileSync(new URL('./schema.sql', import.meta.url), 'utf8');
  await pool.query(sql);
}

export async function resetTables(): Promise<void> {
  await pool.query('TRUNCATE processed_events, clicks, orders, attributions, ledger');
}
