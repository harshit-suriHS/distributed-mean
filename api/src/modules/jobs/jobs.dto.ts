import { z } from "zod";
import type { JobSummary, JobView } from "./jobs.repo.js";

// ---- Requests ----

// Problem statement bounds: 1 < F < 100k, 1 < C < 1000k
export const CreateJobRequest = z.strictObject({
  upload_id: z.uuid(),
  no_of_files: z.int().min(2).max(99_999),
  no_of_columns: z.int().min(2).max(999_999),
});
export type CreateJobRequest = z.infer<typeof CreateJobRequest>;

export const JobIdParams = z.strictObject({ id: z.uuid() });

export const ListJobsQuery = z.strictObject({
  status: z.enum(["validating", "queued", "running", "succeeded", "failed"]).optional(),
  before: z.uuid().optional(), // the `next` cursor of the previous page
  limit: z.coerce.number().int().min(1).max(200).default(50),
});
export type ListJobsQuery = z.infer<typeof ListJobsQuery>;

// ---- Responses ----

const JobStatus = z.enum(["validating", "queued", "running", "succeeded", "failed"]);
const Timestamp = z.iso.datetime();

export const CreateJobResponse = z.object({
  id: z.uuid(),
  status: JobStatus,
});
export type CreateJobResponse = z.infer<typeof CreateJobResponse>;

export const JobResponse = z.object({
  id: z.uuid(),
  status: JobStatus,
  no_of_files: z.int(),
  no_of_columns: z.int(),
  files_per_task: z.int().nullable(),
  progress: z.object({
    files_done: z.int(),
    percent: z.int().min(0).max(100),
  }),
  tasks: z.object({
    pending: z.int(),
    running: z.int(),
    done: z.int(),
    failed: z.int(),
    skipped: z.int(),
  }),
  failure_type: z.enum(["internal", "user_input"]).nullable(),
  error: z.string().nullable(),
  created_at: Timestamp,
  started_at: Timestamp.nullable(),
  completed_at: Timestamp.nullable(),
});
export type JobResponse = z.infer<typeof JobResponse>;

export const JobSummaryResponse = z.object({
  id: z.uuid(),
  status: JobStatus,
  no_of_files: z.int(),
  no_of_columns: z.int(),
  percent: z.int().min(0).max(100),
  failure_type: z.enum(["internal", "user_input"]).nullable(),
  created_at: Timestamp,
  completed_at: Timestamp.nullable(),
});

export const ListJobsResponse = z.object({
  jobs: z.array(JobSummaryResponse),
  next: z.uuid().nullable(), // pass as ?before= for the next page; null on the last page
});

export const ResultResponse = z.object({
  url: z.url(),
  expires_at: Timestamp,
});

// ---- Mappers: DB row → response ----

const iso = (d: Date | null) => (d ? d.toISOString() : null);

export function toJobResponse(job: JobView): z.input<typeof JobResponse> {
  return {
    id: job.uuid,
    status: job.status,
    no_of_files: job.no_of_files,
    no_of_columns: job.no_of_columns,
    files_per_task: job.files_per_task,
    progress: {
      files_done: job.files_done,
      percent: Math.floor((job.files_done / job.no_of_files) * 100),
    },
    tasks: {
      pending: job.pending,
      running: job.running,
      done: job.done,
      failed: job.failed,
      skipped: job.skipped,
    },
    failure_type: job.failure_type,
    error: job.error,
    created_at: job.created_at.toISOString(),
    started_at: iso(job.started_at),
    completed_at: iso(job.completed_at),
  };
}

export function toListJobsResponse(
  jobs: JobSummary[],
  limit: number,
): z.input<typeof ListJobsResponse> {
  return {
    jobs: jobs.map((j) => ({
      id: j.uuid,
      status: j.status,
      no_of_files: j.no_of_files,
      no_of_columns: j.no_of_columns,
      percent: Math.floor((j.files_done / j.no_of_files) * 100),
      failure_type: j.failure_type,
      created_at: j.created_at.toISOString(),
      completed_at: iso(j.completed_at),
    })),
    next: jobs.length === limit ? (jobs[jobs.length - 1]?.uuid ?? null) : null,
  };
}
