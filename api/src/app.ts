import express from "express";
import type pg from "pg";
import { pinoHttp } from "pino-http";
import type { Config } from "./config.js";
import { errorHandler, notFound } from "./lib/errors.js";
import type { Logger } from "./lib/logger.js";
import { jobsRouter } from "./modules/jobs/jobs.routes.js";
import { uploadsRouter } from "./modules/uploads/uploads.routes.js";
import type { Storage } from "./storage/s3.js";

export interface Deps {
  config: Config;
  pool: pg.Pool;
  storage: Storage;
  logger: Logger;
}

/** Builds the app without listening, so tests can drive it directly. */
export function createApp(deps: Deps) {
  const app = express();
  app.disable("x-powered-by");
  app.use(pinoHttp({ logger: deps.logger }));
  app.use(express.json({ limit: "100kb" }));

  app.get("/health", async (_req, res) => {
    await Promise.all([deps.pool.query("SELECT 1"), deps.storage.ping()]);
    res.json({ status: "ok" });
  });
  app.use("/uploads", uploadsRouter(deps));
  app.use("/jobs", jobsRouter(deps));

  app.use(() => {
    throw notFound("route");
  });
  app.use(errorHandler(deps.logger));
  return app;
}
