import { createApp } from "./app.js";
import { loadConfig } from "./config.js";
import { createPool } from "./db/pool.js";
import { createLogger } from "./lib/logger.js";
import { createOrchestratorClient } from "./orchestrator/client.js";
import { createStorage } from "./storage/s3.js";

const config = loadConfig();
const logger = createLogger(config.logLevel);
const pool = createPool(config.db);
const storage = createStorage(config.s3);
const orchestrator = createOrchestratorClient(config.orchestratorAddr);

const server = createApp({ config, pool, storage, logger, orchestrator }).listen(
  config.port,
  () => {
    logger.info({ port: config.port }, "api listening");
  },
);

function shutdown(signal: string) {
  logger.info({ signal }, "shutting down");
  server.close(() => {
    orchestrator.close();
    pool.end().finally(() => process.exit(0));
  });
}
process.on("SIGTERM", () => shutdown("SIGTERM"));
process.on("SIGINT", () => shutdown("SIGINT"));
