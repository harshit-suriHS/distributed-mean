import { type Db, onlyRow } from "../../db/pool.js";
import type { StoredObject } from "../../storage/s3.js";

export type JobStatus = "queued" | "running" | "succeeded" | "failed";
export type FailureType = "internal" | "user_input";

export async function insertJob(db: Db, noOfFiles: number, noOfColumns: number) {
  const { rows } = await db.query<{ id: string; uuid: string; status: JobStatus }>(
    `INSERT INTO jobs (no_of_files, no_of_columns) VALUES ($1, $2)
     RETURNING id, uuid, status`,
    [noOfFiles, noOfColumns],
  );
  return onlyRow(rows);
}

/** One statement for all inputs: up to ~100k rows as two arrays. */
export async function insertInputFiles(db: Db, jobId: string, objects: StoredObject[]) {
  await db.query(
    `INSERT INTO files (job_id, type, key, no_of_files, size_bytes)
     SELECT $1, 'input', key, 1, size
       FROM unnest($2::text[], $3::bigint[]) AS t(key, size)`,
    [jobId, objects.map((o) => o.key), objects.map((o) => o.size)],
  );
}

export async function notifyNewJob(db: Db, jobId: string) {
  // Delivered to listeners only when the surrounding transaction commits
  await db.query("SELECT pg_notify('new_job', $1)", [jobId]);
}

export interface JobView {
  uuid: string;
  status: JobStatus;
  no_of_files: number;
  no_of_columns: number;
  files_per_task: number | null;
  failure_type: FailureType | null;
  error: string | null;
  created_at: Date;
  started_at: Date | null;
  completed_at: Date | null;
  files_done: number;
  pending: number;
  running: number;
  done: number;
  failed: number;
  skipped: number;
}

export async function findJobByUuid(db: Db, uuid: string) {
  const { rows } = await db.query<JobView>(
    `SELECT j.uuid, j.status, j.no_of_files, j.no_of_columns, j.files_per_task,
            j.failure_type, j.error, j.created_at, j.started_at, j.completed_at,
            coalesce(sum(t.no_of_files) FILTER (WHERE t.kind = 'map' AND t.status = 'done'), 0)::int
              AS files_done,
            count(t.id) FILTER (WHERE t.status = 'pending')::int AS pending,
            count(t.id) FILTER (WHERE t.status = 'running')::int AS running,
            count(t.id) FILTER (WHERE t.status = 'done')::int    AS done,
            count(t.id) FILTER (WHERE t.status = 'failed')::int  AS failed,
            count(t.id) FILTER (WHERE t.status = 'skipped')::int AS skipped
       FROM jobs j
       LEFT JOIN tasks t ON t.job_id = j.id
      WHERE j.uuid = $1
      GROUP BY j.id`,
    [uuid],
  );
  return rows[0];
}

export interface JobSummary {
  uuid: string;
  status: JobStatus;
  no_of_files: number;
  no_of_columns: number;
  files_done: number;
  failure_type: FailureType | null;
  created_at: Date;
  completed_at: Date | null;
}

/** Newest first; keyset pagination on the time-ordered UUIDv7 (`before` = last id seen). */
export async function listJobs(
  db: Db,
  { status, before, limit }: { status?: JobStatus; before?: string; limit: number },
) {
  const { rows } = await db.query<JobSummary>(
    `SELECT j.uuid, j.status, j.no_of_files, j.no_of_columns, j.failure_type,
            j.created_at, j.completed_at,
            coalesce((SELECT sum(t.no_of_files) FROM tasks t
                       WHERE t.job_id = j.id AND t.kind = 'map' AND t.status = 'done'), 0)::int
              AS files_done
       FROM jobs j
      WHERE ($1::text IS NULL OR j.status = $1)
        AND ($2::uuid IS NULL OR j.uuid < $2)
      ORDER BY j.uuid DESC
      LIMIT $3`,
    [status ?? null, before ?? null, limit],
  );
  return rows;
}

export async function findResult(db: Db, jobUuid: string) {
  const { rows } = await db.query<{ status: JobStatus; key: string | null }>(
    `SELECT j.status, f.key
       FROM jobs j
       LEFT JOIN files f ON f.job_id = j.id AND f.type = 'result' AND f.deleted_at IS NULL
      WHERE j.uuid = $1`,
    [jobUuid],
  );
  return rows[0];
}
