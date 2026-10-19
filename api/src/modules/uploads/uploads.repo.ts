import { type Db, onlyRow } from "../../db/pool.js";

export interface UploadRow {
  id: string; // bigint comes back as a string from pg
  uuid: string;
  status: "open" | "committed" | "expired";
  expires_at: Date;
  expired: boolean;
}

export async function insertUpload(db: Db, ttlHours: number) {
  const { rows } = await db.query<{ uuid: string; expires_at: Date }>(
    `INSERT INTO uploads (expires_at) VALUES (now() + $1 * interval '1 hour')
     RETURNING uuid, expires_at`,
    [ttlHours],
  );
  return onlyRow(rows);
}

export async function findUploadByUuid(db: Db, uuid: string) {
  const { rows } = await db.query<UploadRow>(
    `SELECT id, uuid, status, expires_at, expires_at <= now() AS expired
       FROM uploads WHERE uuid = $1`,
    [uuid],
  );
  return rows[0];
}

/** Commit an open, unexpired upload to a job. False if it was committed or expired meanwhile. */
export async function commitUpload(db: Db, uploadId: string, jobId: string) {
  const { rowCount } = await db.query(
    `UPDATE uploads SET status = 'committed', job_id = $2
      WHERE id = $1 AND status = 'open' AND expires_at > now()`,
    [uploadId, jobId],
  );
  return rowCount === 1;
}
