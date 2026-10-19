import pg from "pg";
import type { Config } from "../config.js";

export type Db = pg.Pool | pg.PoolClient;

export function createPool(config: Config["db"]) {
  return new pg.Pool({ ...config, max: 10 });
}

/** The single row of an INSERT ... RETURNING; a missing row is a bug, not a client error. */
export function onlyRow<T>(rows: T[]): T {
  const row = rows[0];
  if (row === undefined) throw new Error("expected exactly one row");
  return row;
}

/** Runs fn in one transaction; rolls back if fn throws. */
export async function withTransaction<T>(
  pool: pg.Pool,
  fn: (tx: pg.PoolClient) => Promise<T>,
): Promise<T> {
  const client = await pool.connect();
  try {
    await client.query("BEGIN");
    const result = await fn(client);
    await client.query("COMMIT");
    return result;
  } catch (err) {
    await client.query("ROLLBACK");
    throw err;
  } finally {
    client.release();
  }
}
