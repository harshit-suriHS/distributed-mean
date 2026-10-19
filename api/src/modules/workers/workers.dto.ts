import { z } from "zod";
import type { WorkersSnapshot } from "../../orchestrator/client.js";

// ---- Requests ----

export const PutWorkersRequest = z.strictObject({
  desired_workers: z.int().min(0).max(10_000),
});

// ---- Responses ----

const WorkerResponse = z.object({
  id: z.string(),
  status: z.enum(["busy", "idle"]),
  task: z
    .object({
      id: z.string(),
      job_id: z.uuid().nullable(),
      kind: z.enum(["map", "merge"]),
      attempt: z.int(),
      seconds: z.number(),
    })
    .nullable(),
  seconds_since_seen: z.number(),
});

export const WorkersResponse = z.object({
  desired_workers: z.int(),
  live: z.int(),
  busy: z.int(),
  idle: z.int(),
  ready_tasks: z.int(),
  workers: z.array(WorkerResponse),
});

export const DesiredWorkersResponse = z.object({ desired_workers: z.int() });

// ---- Mappers ----

const round = (n: number) => Math.round(n * 10) / 10;

export function toWorkersResponse(
  desired: number,
  snapshot: WorkersSnapshot,
  jobUuids: Map<string, string>,
): z.input<typeof WorkersResponse> {
  const workers = snapshot.workers.map((w) => ({
    id: w.workerId,
    status: w.busy ? ("busy" as const) : ("idle" as const),
    task: w.busy
      ? {
          id: w.taskId,
          job_id: jobUuids.get(w.jobId) ?? null,
          kind: w.kind as "map" | "merge",
          attempt: w.attempt,
          seconds: round(w.secondsOnTask),
        }
      : null,
    seconds_since_seen: round(w.secondsSinceSeen),
  }));
  const busy = workers.filter((w) => w.status === "busy").length;
  return {
    desired_workers: desired,
    live: workers.length,
    busy,
    idle: workers.length - busy,
    ready_tasks: snapshot.readyTasks,
    workers,
  };
}
