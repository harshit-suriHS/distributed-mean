import { Router } from "express";
import type { Deps } from "../../app.js";
import { send } from "../../lib/http.js";
import { DesiredWorkersResponse, PutWorkersRequest, WorkersResponse } from "./workers.dto.js";
import { getWorkers, putDesiredWorkers } from "./workers.service.js";

export function workersRouter(deps: Deps) {
  const router = Router();

  router.get("/", async (_req, res) => {
    send(res, 200, WorkersResponse, await getWorkers(deps));
  });

  router.put("/", async (req, res) => {
    const { desired_workers } = PutWorkersRequest.parse(req.body);
    send(res, 200, DesiredWorkersResponse, await putDesiredWorkers(deps, desired_workers));
  });

  return router;
}
