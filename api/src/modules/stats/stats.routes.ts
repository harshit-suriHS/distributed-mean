import { Router } from "express";
import type { Deps } from "../../app.js";
import { send } from "../../lib/http.js";
import { StatsResponse, toStatsResponse } from "./stats.dto.js";
import { loadStats } from "./stats.repo.js";

export function statsRouter({ pool }: Deps) {
  const router = Router();

  router.get("/", async (_req, res) => {
    send(res, 200, StatsResponse, toStatsResponse(await loadStats(pool)));
  });

  return router;
}
