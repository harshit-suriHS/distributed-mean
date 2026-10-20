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
      <Stat label="workers busy" value={workers?.busy ?? dash} tone="blue" />
      <Stat label="workers idle" value={workers?.idle ?? dash} />
      <Stat label="jobs succeeded" value={stats?.jobs.succeeded ?? dash} tone="green" />
      <Stat label="jobs failed" value={stats?.jobs.failed ?? dash} tone="red" />
    </div>
  );
}
