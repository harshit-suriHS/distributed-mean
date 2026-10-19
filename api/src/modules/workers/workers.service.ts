import type { Deps } from "../../app.js";
import type { WorkersSnapshot } from "../../orchestrator/client.js";
import { toWorkersResponse } from "./workers.dto.js";
import { getDesiredWorkers, jobUuids, setDesiredWorkers } from "./workers.repo.js";

/** Live worker state comes from the orchestrator (memory); W from Postgres. */
export async function getWorkers(deps: Deps) {
  return workersView(deps, await deps.orchestrator.listWorkers());
}

/** Builds the response from a snapshot already fetched (the SSE stream reuses its own). */
export async function workersView({ pool }: Deps, snapshot: WorkersSnapshot) {
  const desired = await getDesiredWorkers(pool);
  const uuids = await jobUuids(
    pool,
    snapshot.workers.filter((w) => w.busy).map((w) => w.jobId),
  );
  return toWorkersResponse(desired, snapshot, uuids);
}

export async function putDesiredWorkers({ pool }: Deps, desired: number) {
  return { desired_workers: await setDesiredWorkers(pool, desired) };
}
