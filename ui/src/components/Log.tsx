import type { LogLine } from "../api";

export function Log({ lines }: { lines: LogLine[] }) {
  return (
    <>
      <h2>Live log</h2>
      <div className="log">
        {lines.length === 0 && <p className="muted">events appear here as they happen</p>}
        {lines.map((line, i) => (
          <div key={`${line.at}-${i}`} className={`line ${line.level}`}>
            <span className="muted">{new Date(line.at).toLocaleTimeString()}</span> {line.message}
          </div>
        ))}
      </div>
    </>
  );
}
