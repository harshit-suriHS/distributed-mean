import type { Stats, WorkersView } from "../api";

function Stat({ label, value, tone }: { label: string; value: number | string; tone?: string }) {
  return (
    <div className={`stat ${tone ?? ""}`}>
      <div className="value">{value}</div>
      <div className="label">{label}</div>
    </div>
  );
}

export function StatsBar({ stats, workers }: { stats: Stats | null; workers: WorkersView | null }) {
  const dash = "–";
  return (
    <div className="stats">
      <Stat
        label="workers busy / live"
        value={workers ? `${workers.busy} / ${workers.live}` : dash}
      />
      <Stat
        label="jobs validating / queued"
        value={stats ? `${stats.jobs.validating} / ${stats.jobs.queued}` : dash}
      />
      <Stat label="jobs running" value={stats?.jobs.running ?? dash} tone="blue" />
      <Stat label="jobs succeeded" value={stats?.jobs.succeeded ?? dash} tone="green" />
      <Stat label="jobs failed" value={stats?.jobs.failed ?? dash} tone="red" />
      <Stat label="tasks waiting" value={stats?.tasks_of_active_jobs.pending ?? dash} />
      <Stat label="tasks done / min" value={stats?.last_minute.tasks_done ?? dash} />
      <Stat label="files summed / min" value={stats?.last_minute.files_summed ?? dash} />
    </div>
  );
}
