import { type Db, onlyRow } from "../../db/pool.js";

export async function getDesiredWorkers(db: Db) {
  const { rows } = await db.query<{ desired_workers: number }>(
    "SELECT desired_workers FROM settings WHERE id = 1",
  );
  return onlyRow(rows).desired_workers;
}

/** Sets W and tells the supervisor (NOTIFY settings_changed) so it converges right away. */
export async function setDesiredWorkers(db: Db, desired: number) {
  const { rows } = await db.query<{ desired_workers: number }>(
    "UPDATE settings SET desired_workers = $1 WHERE id = 1 RETURNING desired_workers",
    [desired],
  );
  await db.query("SELECT pg_notify('settings_changed', $1)", [String(desired)]);
  return onlyRow(rows).desired_workers;
}

/** Internal job ids → public uuids (the orchestrator only knows internal ids). */
export async function jobUuids(db: Db, ids: string[]) {
  if (ids.length === 0) return new Map<string, string>();
  const { rows } = await db.query<{ id: string; uuid: string }>(
    "SELECT id, uuid FROM jobs WHERE id = ANY($1::bigint[])",
    [ids],
  );
  return new Map(rows.map((r) => [r.id, r.uuid]));
}
