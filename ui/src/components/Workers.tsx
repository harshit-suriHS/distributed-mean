import { type FormEvent, useState } from "react";
import type { WorkersView } from "../api";

export function Workers({
  view,
  onScale,
}: {
  view: WorkersView | null;
  onScale: (desired: number) => Promise<unknown>;
}) {
  const [error, setError] = useState("");
  const [draft, setDraft] = useState<string | null>(null); // null: show the current W
  const [applying, setApplying] = useState(false);
  const current = view?.desired_workers;
  const input = draft ?? (current === undefined ? "" : String(current));

  const wanted = Number(input);
  const valid = input !== "" && Number.isInteger(wanted) && wanted >= 0 && wanted <= 10_000;

  const apply = async (e: FormEvent) => {
    e.preventDefault();
    if (!valid) return;
    setError("");
    setApplying(true);
    try {
      await onScale(wanted);
      setDraft(null);
    } catch (err) {
      setError((err as Error).message);
    } finally {
      setApplying(false);
    }
  };

  return (
    <>
      <div className="row">
        <h2>Workers</h2>
        {view && (
          <form className="scale" onSubmit={apply}>
            <label className="muted" htmlFor="desired-workers">
              W
            </label>
            <input
              id="desired-workers"
              type="number"
              min={0}
              max={10000}
              step={1}
              value={input}
              onChange={(e) => setDraft(e.target.value)}
            />
            <button type="submit" disabled={!valid || applying || wanted === current}>
              {applying ? "applying…" : "apply"}
            </button>
          </form>
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
