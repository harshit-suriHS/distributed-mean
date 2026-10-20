// Thin client for the public API (served under /api by the Vite proxy or nginx).

export type JobStatus = "validating" | "queued" | "running" | "succeeded" | "failed";

export interface JobSummary {
  id: string;
  status: JobStatus;
  no_of_files: number;
  no_of_columns: number;
  percent: number;
  failure_type: "internal" | "user_input" | null;
  created_at: string;
  completed_at: string | null;
}

export interface Job extends JobSummary {
  error: string | null;
  tasks: Record<"pending" | "running" | "done" | "failed" | "skipped", number>;
}

export interface Worker {
  id: string;
  status: "busy" | "idle";
  task: {
    id: string;
    job_id: string | null;
    kind: "map" | "merge";
    attempt: number;
    seconds: number;
  } | null;
  seconds_since_seen: number;
}

export interface WorkersView {
  desired_workers: number;
  live: number;
  busy: number;
  idle: number;
  ready_tasks: number;
  workers: Worker[];
}

export interface Stats {
  jobs: Record<JobStatus, number>;
  tasks_of_active_jobs: Record<"pending" | "running" | "done" | "failed" | "skipped", number>;
  last_minute: { tasks_done: number; files_summed: number };
}

export interface LogLine {
  at: string;
  level: "info" | "warn";
  message: string;
}

async function call<T>(path: string, init?: RequestInit): Promise<T> {
  const res = await fetch(`/api${path}`, init);
  const body = await res.json().catch(() => ({}));
  if (!res.ok) throw new Error(body?.error?.message ?? `${res.status} ${res.statusText}`);
  return body as T;
}

export const api = {
  jobs: () => call<{ jobs: JobSummary[] }>("/jobs?limit=30").then((r) => r.jobs),
  job: (id: string) => call<Job>(`/jobs/${id}`),
  stats: () => call<Stats>("/stats"),
  result: (id: string) => call<{ url: string }>(`/jobs/${id}/result`),
  setWorkers: (desired: number) =>
    call<{ desired_workers: number }>("/workers", {
      method: "PUT",
      headers: { "content-type": "application/json" },
      body: JSON.stringify({ desired_workers: desired }),
    }),
  events: () => new EventSource("/api/events"),

  /** The three-step upload: open an upload, POST each file to MinIO, then commit as a job. */
  async submit(
    files: { name: string; body: Blob }[],
    columns: number,
    onProgress: (done: number) => void,
  ): Promise<string> {
    const upload = await call<{
      upload_id: string;
      prefix: string;
      url: string;
      fields: Record<string, string>;
    }>("/uploads", { method: "POST" });
    let done = 0;
    const queue = [...files];
    const send = async () => {
      for (let file = queue.shift(); file; file = queue.shift()) {
        const form = new FormData();
        for (const [k, v] of Object.entries(upload.fields)) form.append(k, v);
        form.set("key", `${upload.prefix}${file.name}`);
        form.append("file", file.body);
        const res = await fetch(upload.url, { method: "POST", body: form });
        if (!res.ok) throw new Error(`upload of ${file.name} failed: ${res.status}`);
        onProgress(++done);
      }
    };
    await Promise.all(Array.from({ length: 8 }, send)); // 8 uploads in parallel
    const job = await call<{ id: string }>("/jobs", {
      method: "POST",
      headers: { "content-type": "application/json" },
      body: JSON.stringify({
        upload_id: upload.upload_id,
        no_of_files: files.length,
        no_of_columns: columns,
      }),
    });
    return job.id;
  },
};
