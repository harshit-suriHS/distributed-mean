import { type Db, onlyRow } from "../../db/pool.js";

export interface StatsRow {
  jobs: Record<string, number> | null;
  tasks: Record<string, number> | null;
  tasks_done: number;
  files_summed: number;
}

/** Everything from Postgres, so stats keep working while the orchestrator restarts. */
export async function loadStats(db: Db) {
  const { rows } = await db.query<StatsRow>(
    `SELECT
       (SELECT json_object_agg(status, n)
          FROM (SELECT status, count(*)::int AS n FROM jobs GROUP BY status) s) AS jobs,
       (SELECT json_object_agg(status, n)
          FROM (SELECT t.status, count(*)::int AS n FROM tasks t JOIN jobs j ON j.id = t.job_id
                 WHERE j.status IN ('queued', 'running') GROUP BY t.status) s) AS tasks,
       (SELECT count(*)::int FROM tasks
         WHERE status = 'done' AND completed_at > now() - interval '1 minute') AS tasks_done,
       (SELECT coalesce(sum(no_of_files), 0)::int FROM tasks
         WHERE kind = 'map' AND status = 'done'
           AND completed_at > now() - interval '1 minute') AS files_summed`,
  );
  return onlyRow(rows);
}
