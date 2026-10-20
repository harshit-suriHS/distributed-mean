import { z } from "zod";
import type { StatsRow } from "./stats.repo.js";

export const StatsResponse = z.object({
  jobs: z.object({
    validating: z.int(),
    queued: z.int(),
    running: z.int(),
    succeeded: z.int(),
    failed: z.int(),
  }),
  tasks_of_active_jobs: z.object({
    pending: z.int(),
    running: z.int(),
    done: z.int(),
    failed: z.int(),
    skipped: z.int(),
  }),
  last_minute: z.object({ tasks_done: z.int(), files_summed: z.int() }),
});

const JOB_STATES = ["validating", "queued", "running", "succeeded", "failed"] as const;
const TASK_STATES = ["pending", "running", "done", "failed", "skipped"] as const;

function counts<K extends string>(row: Record<string, number> | null, keys: readonly K[]) {
  return Object.fromEntries(keys.map((k) => [k, row?.[k] ?? 0])) as Record<K, number>;
}

export function toStatsResponse(row: StatsRow): z.input<typeof StatsResponse> {
  return {
    jobs: counts(row.jobs, JOB_STATES),
    tasks_of_active_jobs: counts(row.tasks, TASK_STATES),
    last_minute: { tasks_done: row.tasks_done, files_summed: row.files_summed },
  };
}
