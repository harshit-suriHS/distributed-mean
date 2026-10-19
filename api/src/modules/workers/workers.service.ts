import type { Deps } from "../../app.js";
import { toWorkersResponse } from "./workers.dto.js";
import { getDesiredWorkers, jobUuids, setDesiredWorkers } from "./workers.repo.js";

/** Live worker state comes from the orchestrator (memory); W from Postgres. */
export async function getWorkers({ pool, orchestrator }: Deps) {
  const [desired, snapshot] = await Promise.all([
    getDesiredWorkers(pool),
    orchestrator.listWorkers(),
  ]);
  const uuids = await jobUuids(
    pool,
    snapshot.workers.filter((w) => w.busy).map((w) => w.jobId),
  );
  return toWorkersResponse(desired, snapshot, uuids);
}

export async function putDesiredWorkers({ pool }: Deps, desired: number) {
  return { desired_workers: await setDesiredWorkers(pool, desired) };
}
