import { Router } from "express";
import type { Deps } from "../../app.js";
import { send } from "../../lib/http.js";
import {
  CreateJobRequest,
  CreateJobResponse,
  JobIdParams,
  JobResponse,
  ListJobsQuery,
  ListJobsResponse,
  ResultResponse,
  toJobResponse,
  toListJobsResponse,
} from "./jobs.dto.js";
import { createJob, getJob, getResultLink, listAllJobs } from "./jobs.service.js";

export function jobsRouter(deps: Deps) {
  const router = Router();

  router.post("/", async (req, res) => {
    const job = await createJob(deps, CreateJobRequest.parse(req.body));
    res.location(`/jobs/${job.id}`);
    send(res, 202, CreateJobResponse, job);
  });

  router.get("/", async (req, res) => {
    const query = ListJobsQuery.parse(req.query);
    send(
      res,
      200,
      ListJobsResponse,
      toListJobsResponse(await listAllJobs(deps, query), query.limit),
    );
  });

  router.get("/:id/result", async (req, res) => {
    const { id } = JobIdParams.parse(req.params);
    send(res, 200, ResultResponse, await getResultLink(deps, id));
  });

  router.get("/:id", async (req, res) => {
    const { id } = JobIdParams.parse(req.params);
    send(res, 200, JobResponse, toJobResponse(await getJob(deps, id)));
  });

  return router;
}
