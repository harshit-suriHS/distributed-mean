/**
 * The 'validating' phase of a job: before anything runs, check every uploaded file is there.
 *
 * POST /jobs returns as soon as the job exists; this lists the upload's prefix in the
 * background (100K files = 100 LIST calls: seconds on S3, minutes on a single-node MinIO) and
 * only then queues the job for the orchestrator. A missing or impossible file fails the job
 * before any compute is spent on it, instead of in whichever task reads it.
 *
 * No lock is held while listing. A short lease (validating_until) says who is doing it; if that
 * API stops, the lease lapses and the sweep (any replica) picks the job up again. Listing changes
 * nothing, and both outcomes are conditional on status = 'validating', so two validations of the
 * same job can't both take effect.
 */
import type { Deps } from "../../app.js";
import { withTransaction } from "../../db/pool.js";
import type { StoredObject } from "../../storage/s3.js";
import { uploadPrefix } from "../uploads/uploads.service.js";
import {
  claimValidation,
  failValidation,
  insertInputFiles,
  type JobToValidate,
  markQueued,
  notifyNewJob,
  unclaimedValidations,
} from "./jobs.repo.js";

const LEASE_SECS = 600; // longer than listing 100K files on local MinIO (~4 min)
const SWEEP_MS = 30_000;
const SHOWN_PROBLEMS = 3;

/** Bytes a line of C numbers can take: C one-digit values ("1,2,…") up to C full doubles. */
export const minBytesFor = (columns: number) => 2 * columns - 1;
export const maxBytesFor = (columns: number) => 26 * columns + 2;

/** Why the uploaded objects can't be this job's input, or null if they can. */
export function problemWith(job: JobToValidate, prefix: string, objects: StoredObject[]) {
  if (objects.length !== job.no_of_files) {
    return `expected ${job.no_of_files} files, found ${objects.length}`;
  }
  const low = minBytesFor(job.no_of_columns);
  const high = maxBytesFor(job.no_of_columns);
  const bad: string[] = [];
  for (const o of objects) {
    const name = o.key.slice(prefix.length);
    if (name.includes("/")) {
      bad.push(`${name} is in a sub-folder`);
    } else if (!name.toLowerCase().endsWith(".csv")) {
      bad.push(`${name} is not a .csv file`);
    } else if (o.size < low || o.size > high) {
      bad.push(`${name} (${o.size} bytes) can't hold ${job.no_of_columns} values`);
    }
  }
  if (bad.length === 0) return null;
  const more = bad.length > SHOWN_PROBLEMS ? ` and ${bad.length - SHOWN_PROBLEMS} more` : "";
  return `${bad.slice(0, SHOWN_PROBLEMS).join("; ")}${more}`;
}

/** Validates one job if no one else is; never throws (a storage error leaves it for the sweep). */
export async function validateJob({ pool, storage, logger }: Deps, jobId: string) {
  try {
    const job = await claimValidation(pool, jobId, LEASE_SECS);
    if (!job) return;

    const prefix = uploadPrefix(job.upload_uuid);
    const started = Date.now();
    const objects = await storage.listPrefix(prefix); // no transaction open
    const problem = problemWith(job, prefix, objects);
    if (problem) {
      await failValidation(pool, job.id, problem);
      logger.info({ jobId, problem }, "job failed validation");
      return;
    }

    await withTransaction(pool, async (tx) => {
      if (!(await markQueued(tx, job.id))) return; // already handled elsewhere
      await insertInputFiles(tx, job.id, objects);
      await notifyNewJob(tx, job.id);
    });
    logger.info({ jobId, files: objects.length, listMs: Date.now() - started }, "job validated");
  } catch (err) {
    logger.warn({ err, jobId }, "validation interrupted, the sweep will retry");
  }
}

/** Picks up validating jobs nobody is working on (API restarts, other replicas). */
export function startValidationSweep(deps: Deps) {
  let running = false;
  const sweep = async () => {
    if (running) return;
    running = true;
    try {
      for (const jobId of await unclaimedValidations(deps.pool)) {
        await validateJob(deps, jobId);
      }
    } catch (err) {
      deps.logger.warn({ err }, "validation sweep failed");
    } finally {
      running = false;
    }
  };
  void sweep();
  const timer = setInterval(sweep, SWEEP_MS);
  return () => clearInterval(timer);
}
