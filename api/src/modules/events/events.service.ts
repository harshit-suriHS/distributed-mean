import type { WorkersSnapshot } from "../../orchestrator/client.js";

export interface LogLine {
  at: string;
  level: "info" | "warn";
  message: string;
}

type Workers = WorkersSnapshot["workers"];

/** What changed between two worker snapshots, as human-readable log lines. */
export function diffWorkers(before: Workers | null, after: Workers, at: string): LogLine[] {
  if (before === null) return [];
  const lines: LogLine[] = [];
  const old = new Map(before.map((w) => [w.workerId, w]));
  const now = new Map(after.map((w) => [w.workerId, w]));
  for (const w of after) {
    const prev = old.get(w.workerId);
    if (!prev) {
      lines.push({ at, level: "info", message: `worker ${w.workerId} joined` });
    }
    if (w.busy && (!prev?.busy || prev.taskId !== w.taskId)) {
      const retry = w.attempt > 1 ? ` (attempt ${w.attempt})` : "";
      lines.push({
        at,
        level: w.attempt > 1 ? "warn" : "info",
        message: `worker ${w.workerId} started ${w.kind} task ${w.taskId}${retry}`,
      });
    }
  }
  for (const w of before) {
    if (!now.has(w.workerId)) {
      lines.push({ at, level: "warn", message: `worker ${w.workerId} left` });
    }
  }
  return lines;
}
