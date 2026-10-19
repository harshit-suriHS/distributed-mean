import { Router } from "express";
import type { Deps } from "../../app.js";
import { send } from "../../lib/http.js";
import {
  CreateJobRequest,
  CreateJobResponse,
  JobIdParams,
  JobResponse,
  toJobResponse,
} from "./jobs.dto.js";
import { createJob, getJob } from "./jobs.service.js";

export function jobsRouter(deps: Deps) {
  const router = Router();

  router.post("/", async (req, res) => {
    const job = await createJob(deps, CreateJobRequest.parse(req.body));
    res.location(`/jobs/${job.id}`);
    send(res, 202, CreateJobResponse, job);
  });

  router.get("/:id", async (req, res) => {
    const { id } = JobIdParams.parse(req.params);
    send(res, 200, JobResponse, toJobResponse(await getJob(deps, id)));
  });

  return router;
}
