import { useState } from "react";
import type { WorkersView } from "../api";

export function Workers({
  view,
  onScale,
}: {
  view: WorkersView | null;
  onScale: (desired: number) => Promise<unknown>;
}) {
  const [error, setError] = useState("");
  const scale = (n: number) => {
    setError("");
    onScale(Math.max(0, n)).catch((e: Error) => setError(e.message));
  };

  return (
    <>
      <div className="row">
        <h2>Workers</h2>
        {view && (
          <div className="scale">
            <span className="muted">W =</span>
            <button type="button" onClick={() => scale(view.desired_workers - 1)}>
              −
            </button>
            <strong>{view.desired_workers}</strong>
            <button type="button" onClick={() => scale(view.desired_workers + 1)}>
              +
            </button>
          </div>
        )}
      </div>
      {error && <p className="error">{error}</p>}
      {!view && <p className="muted">waiting for the orchestrator…</p>}
      {view && view.workers.length === 0 && <p className="muted">no live workers</p>}
      <div className="workers">
        {view?.workers.map((w) => (
          <div
            key={w.id}
            className={`worker ${w.status}`}
            title={`seen ${w.seconds_since_seen}s ago`}
          >
            <div className="worker-id">{w.id}</div>
            {w.task ? (
              <div className="worker-task">
                {w.task.kind} #{w.task.id}
                {w.task.attempt > 1 && <span className="retry"> retry {w.task.attempt}</span>}
                <div className="muted">
                  job {w.task.job_id?.slice(-8) ?? "?"} · {w.task.seconds}s
                </div>
              </div>
            ) : (
              <div className="muted">idle</div>
            )}
          </div>
        ))}
      </div>
    </>
  );
}
