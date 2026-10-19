import { useState } from "react";
import { api } from "../api";

type Mode = "generate" | "upload";

function randomCsv(columns: number): string {
  const values = new Array<string>(columns);
  for (let i = 0; i < columns; i++) values[i] = String(Math.random());
  return `${values.join(",")}\n`;
}

/** Submit a job: generate random files in the browser, or upload your own CSV files. */
export function NewJob({ onSubmitted }: { onSubmitted: (id: string, files: number) => void }) {
  const [mode, setMode] = useState<Mode>("generate");
  const [files, setFiles] = useState(20);
  const [columns, setColumns] = useState(100);
  const [picked, setPicked] = useState<FileList | null>(null);
  const [progress, setProgress] = useState("");
  const [error, setError] = useState("");
  const [busy, setBusy] = useState(false);

  const submit = async () => {
    setError("");
    setBusy(true);
    try {
      let toSend: { name: string; body: Blob }[];
      let c = columns;
      if (mode === "generate") {
        toSend = Array.from({ length: files }, (_, i) => ({
          name: `file_${String(i).padStart(6, "0")}.csv`,
          body: new Blob([randomCsv(columns)], { type: "text/csv" }),
        }));
      } else {
        if (!picked || picked.length < 2) throw new Error("pick at least 2 CSV files");
        toSend = Array.from(picked, (f) => ({ name: f.name, body: f }));
        // C from the first file; the workers check every file has exactly this many values
        c = (await picked[0]?.text())?.trim().split(",").length ?? 0;
      }
      const id = await api.submit(toSend, c, (done) =>
        setProgress(`uploading ${done}/${toSend.length}`),
      );
      setProgress("");
      onSubmitted(id, toSend.length);
    } catch (e) {
      setError((e as Error).message);
      setProgress("");
    } finally {
      setBusy(false);
    }
  };

  return (
    <>
      <h2>New job</h2>
      <div className="tabs">
        <button
          type="button"
          className={mode === "generate" ? "active" : ""}
          onClick={() => setMode("generate")}
        >
          random files
        </button>
        <button
          type="button"
          className={mode === "upload" ? "active" : ""}
          onClick={() => setMode("upload")}
        >
          my CSV files
        </button>
      </div>
      {mode === "generate" ? (
        <div className="form">
          <label>
            F (files)
            <input
              type="number"
              min={2}
              max={99999}
              value={files}
              onChange={(e) => setFiles(Number(e.target.value))}
            />
          </label>
          <label>
            C (values per file)
            <input
              type="number"
              min={2}
              max={999999}
              value={columns}
              onChange={(e) => setColumns(Number(e.target.value))}
            />
          </label>
        </div>
      ) : (
        <div className="form">
          <input
            type="file"
            accept=".csv,text/csv"
            multiple
            onChange={(e) => setPicked(e.target.files)}
          />
          <p className="muted">each file: one line of comma-separated numbers</p>
        </div>
      )}
      <button type="button" className="primary" disabled={busy} onClick={submit}>
        {busy ? progress || "submitting…" : "upload & submit"}
      </button>
      {error && <p className="error">{error}</p>}
    </>
  );
}
