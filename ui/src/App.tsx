import { useCallback, useEffect, useRef, useState } from "react";
import { api, type JobSummary, type LogLine, type Stats, type WorkersView } from "./api";
import { Jobs } from "./components/Jobs";
import { Log } from "./components/Log";
import { NewJob } from "./components/NewJob";
import { StatsBar } from "./components/StatsBar";
import { Workers } from "./components/Workers";

const MAX_LOG = 200;

/** Workers arrive live over SSE; jobs and stats are polled from the API (Postgres). */
export function App() {
  const [workers, setWorkers] = useState<WorkersView | null>(null);
  const [jobs, setJobs] = useState<JobSummary[]>([]);
  const [stats, setStats] = useState<Stats | null>(null);
  const [log, setLog] = useState<LogLine[]>([]);
  const known = useRef(new Map<string, string>()); // job id → last status seen

  const addLog = useCallback(
    (line: LogLine) => setLog((lines) => [line, ...lines].slice(0, MAX_LOG)),
    [],
  );

  useEffect(() => {
    const source = api.events();
    source.addEventListener("workers", (e) => setWorkers(JSON.parse((e as MessageEvent).data)));
    source.addEventListener("log", (e) => addLog(JSON.parse((e as MessageEvent).data)));
    return () => source.close();
  }, [addLog]);

  useEffect(() => {
    let stopped = false;
    const poll = async () => {
      try {
        const [list, s] = await Promise.all([api.jobs(), api.stats()]);
        if (stopped) return;
        const at = new Date().toISOString();
        for (const job of list) {
          const before = known.current.get(job.id);
          if (before !== undefined && before !== job.status) {
            addLog({
              at,
              level: job.status === "failed" ? "warn" : "info",
              message: `job ${job.id.slice(-8)} ${job.status}`,
            });
          }
          known.current.set(job.id, job.status);
        }
        setJobs(list);
        setStats(s);
      } catch {
        // API restarting: try again on the next tick
      }
    };
    void poll();
    const timer = setInterval(poll, 1500);
    return () => {
      stopped = true;
      clearInterval(timer);
    };
  }, [addLog]);

  return (
    <div className="page">
      <header>
        <h1>Distributed Mean</h1>
        <span className="muted">per-index mean of F CSV files across W workers</span>
      </header>
      <StatsBar stats={stats} workers={workers} />
      <div className="grid">
        <section className="card">
          <NewJob
            onSubmitted={(id, files) =>
              addLog({
                at: new Date().toISOString(),
                level: "info",
                message: `job ${id.slice(-8)} submitted (${files} files)`,
              })
            }
          />
        </section>
        <section className="card">
          <Workers view={workers} onScale={(n) => api.setWorkers(n)} />
        </section>
      </div>
      <div className="grid">
        <section className="card wide">
          <Jobs jobs={jobs} />
        </section>
        <section className="card">
          <Log lines={log} />
        </section>
      </div>
    </div>
  );
}
