import type { Deps } from "../../app.js";
import { withTransaction } from "../../db/pool.js";
import { AppError, conflict, notFound } from "../../lib/errors.js";
import { commitUpload, findUploadByUuid } from "../uploads/uploads.repo.js";
import { uploadPrefix } from "../uploads/uploads.service.js";
import type { CreateJobRequest, ListJobsQuery } from "./jobs.dto.js";
import {
  findJobByUuid,
  findResult,
  insertInputFiles,
  insertJob,
  listJobs,
  notifyNewJob,
} from "./jobs.repo.js";

/**
 * The commit step of an upload: check the files are all there, then create the job,
 * its input `files` rows and close the upload in one transaction.
 */
export async function createJob({ pool, storage }: Deps, body: CreateJobRequest) {
  const upload = await findUploadByUuid(pool, body.upload_id);
  if (!upload) throw notFound("upload");
  if (upload.status !== "open") throw conflict(`upload is already ${upload.status}`);
  if (upload.expired) throw conflict("upload has expired");

  const objects = await storage.listPrefix(uploadPrefix(upload.uuid));
  if (objects.length !== body.no_of_files) {
    throw new AppError(
      400,
      "file_count_mismatch",
      `expected ${body.no_of_files} files, found ${objects.length}`,
    );
  }

  const job = await withTransaction(pool, async (tx) => {
    const job = await insertJob(tx, body.no_of_files, body.no_of_columns);
    await insertInputFiles(tx, job.id, objects);
    // Loses to a concurrent commit or expiry: roll back everything
    if (!(await commitUpload(tx, upload.id, job.id))) {
      throw conflict("upload is no longer open");
    }
    await notifyNewJob(tx, job.id);
    return job;
  });

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
