import type { Deps } from "../../app.js";
import { withTransaction } from "../../db/pool.js";
import { conflict, notFound } from "../../lib/errors.js";
import { commitUpload, findUploadByUuid } from "../uploads/uploads.repo.js";
import type { CreateJobRequest, ListJobsQuery } from "./jobs.dto.js";
import { findJobByUuid, findResult, insertJob, listJobs } from "./jobs.repo.js";
import { validateJob } from "./jobs.validation.js";

/**
 * The commit step of an upload: create the job as 'validating' and close the upload, in one
 * short transaction, then return. Checking the files (a listing that can take minutes on MinIO)
 * happens in the background; the job is queued for the orchestrator only once they all check out.
 */
export async function createJob(deps: Deps, body: CreateJobRequest) {
  const { pool } = deps;
  const upload = await findUploadByUuid(pool, body.upload_id);
  if (!upload) throw notFound("upload");
  if (upload.status !== "open") throw conflict(`upload is already ${upload.status}`);
  if (upload.expired) throw conflict("upload has expired");

  const job = await withTransaction(pool, async (tx) => {
    const job = await insertJob(tx, body.no_of_files, body.no_of_columns);
    // Loses to a concurrent commit or expiry: roll back everything. Once committed, the upload
    // can't expire, so cleanup won't delete files we're about to validate.
    if (!(await commitUpload(tx, upload.id, job.id))) {
      throw conflict("upload is no longer open");
    }
    return job;
  });

  void validateJob(deps, job.id); // not awaited: the client polls GET /jobs/:id
  return { id: job.uuid, status: job.status };
}

export async function getJob({ pool }: Deps, uuid: string) {
  const job = await findJobByUuid(pool, uuid);
  if (!job) throw notFound("job");
  return job;
}

export async function listAllJobs({ pool }: Deps, query: ListJobsQuery) {
  return listJobs(pool, query);
}

const RESULT_LINK_SECONDS = 3600;

/** A presigned link to the result CSV; only once the job has succeeded. */
export async function getResultLink({ pool, storage }: Deps, uuid: string) {
  const row = await findResult(pool, uuid);
  if (!row) throw notFound("job");
  if (row.status !== "succeeded" || !row.key) {
    throw conflict(`job is ${row.status}; the result exists once it has succeeded`);
  }
  return {
    url: await storage.presignDownload(row.key, RESULT_LINK_SECONDS),
    expires_at: new Date(Date.now() + RESULT_LINK_SECONDS * 1000).toISOString(),
  };
}
