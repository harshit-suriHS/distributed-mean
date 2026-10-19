import { describe, expect, it } from "vitest";
import { diffWorkers } from "../src/modules/events/events.service.js";

const w = (id: string, busy = false, taskId = "0", attempt = 0) => ({
  workerId: id,
  busy,
  taskId,
  jobId: "1",
  kind: "map",
  attempt,
  secondsOnTask: 0,
  secondsSinceSeen: 0,
});

describe("diffWorkers", () => {
  it("says nothing on the first snapshot", () => {
    expect(diffWorkers(null, [w("a")], "t")).toEqual([]);
  });

  it("reports joins, new tasks, retries and departures", () => {
    const lines = diffWorkers([w("a"), w("b", true, "5", 1)], [w("a", true, "6", 3), w("c")], "t");
    expect(lines.map((l) => [l.level, l.message])).toEqual([
      ["warn", "worker a started map task 6 (attempt 3)"],
      ["info", "worker c joined"],
      ["warn", "worker b left"],
    ]);
  });

  it("does not repeat a task the worker was already running", () => {
    expect(diffWorkers([w("a", true, "5", 1)], [w("a", true, "5", 1)], "t")).toEqual([]);
  });
});
