import { Router } from "express";
import type { Deps } from "../../app.js";
import type { WorkersSnapshot } from "../../orchestrator/client.js";
import { WorkersResponse } from "../workers/workers.dto.js";
import { workersView } from "../workers/workers.service.js";
import { diffWorkers } from "./events.service.js";

const TICK_MS = 1000;

/**
 * Server-sent events for the UI's live worker view: a `workers` snapshot every second and
 * `log` lines for what changed (joined, started a task, left). Job state is polled separately
 * (GET /jobs), so this stream only carries what lives in the orchestrator's memory.
 */
export function eventsRouter(deps: Deps) {
  const router = Router();

  router.get("/", (req, res) => {
    res.set({
      "content-type": "text/event-stream",
      "cache-control": "no-cache",
      connection: "keep-alive",
      "x-accel-buffering": "no", // nginx: don't buffer the stream
    });
    res.flushHeaders();

    const sendEvent = (event: string, data: unknown) => {
      res.write(`event: ${event}\ndata: ${JSON.stringify(data)}\n\n`);
    };
    let previous: WorkersSnapshot["workers"] | null = null;
    let reachable = true;
    let busy = false;

    const tick = async () => {
      if (busy) return; // a slow orchestrator call: skip this tick rather than pile up
      busy = true;
      try {
        const raw = await deps.orchestrator.listWorkers();
        const snapshot = await workersView(deps, raw);
        const at = new Date().toISOString();
        if (!reachable) sendEvent("log", { at, level: "info", message: "orchestrator is back" });
        reachable = true;
        for (const line of diffWorkers(previous, raw.workers, at)) sendEvent("log", line);
        previous = raw.workers;
        sendEvent("workers", WorkersResponse.parse(snapshot));
      } catch {
        if (reachable) {
          sendEvent("log", {
            at: new Date().toISOString(),
            level: "warn",
            message: "orchestrator unreachable (restarting?): live worker view paused",
          });
        }
        reachable = false;
      } finally {
        busy = false;
      }
    };

    void tick();
    const timer = setInterval(() => void tick(), TICK_MS);
    req.on("close", () => clearInterval(timer));
  });

  return router;
}
