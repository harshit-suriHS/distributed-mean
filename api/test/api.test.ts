// Integration tests: need `docker compose up` (Postgres, MinIO, migrations applied).
import { DeleteObjectsCommand, PutObjectCommand, S3Client } from "@aws-sdk/client-s3";
import pg from "pg";
import request from "supertest";
import { afterAll, beforeAll, describe, expect, it } from "vitest";
import { createApp } from "../src/app.js";
import { loadConfig } from "../src/config.js";
import { createPool } from "../src/db/pool.js";
import { AppError } from "../src/lib/errors.js";
import { createLogger } from "../src/lib/logger.js";
import { problemWith, validateJob } from "../src/modules/jobs/jobs.validation.js";
import type { OrchestratorClient, WorkersSnapshot } from "../src/orchestrator/client.js";
import { createStorage } from "../src/storage/s3.js";

const config = loadConfig();
const pool = createPool(config.db);
const storage = createStorage(config.s3);
// The orchestrator's live view is faked here (its gRPC client has its own test)
let workersSnapshot: WorkersSnapshot | Error = { workers: [], readyTasks: 0 };
const orchestrator: OrchestratorClient = {
  listWorkers: async () => {
    if (workersSnapshot instanceof Error) throw workersSnapshot;
    return workersSnapshot;
  },
  close: () => {},
};
const deps = { config, pool, storage, logger: createLogger("silent"), orchestrator };
const app = createApp(deps);

// Superuser connection: only for assertions, cleanup and forcing expiry
const admin = new pg.Client({
  host: config.db.host,
  port: config.db.port,
  database: config.db.database,
  user: process.env.POSTGRES_USER,
  password: process.env.POSTGRES_PASSWORD,
});
const s3 = new S3Client({
  endpoint: config.s3.endpoint,
  region: config.s3.region,
  credentials: { accessKeyId: config.s3.accessKey, secretAccessKey: config.s3.secretKey },
  forcePathStyle: true,
});

const notifications: string[] = [];
const createdUploads: string[] = [];
const insertedJobIds: string[] = []; // jobs inserted directly (not through an upload)

interface UploadResponse {
  upload_id: string;
  prefix: string;
  url: string;
  fields: Record<string, string>;
}

async function openUpload(): Promise<UploadResponse> {
  const res = await request(app).post("/uploads").expect(201);
  createdUploads.push(res.body.upload_id);
  return res.body;
}

/** What a client does: multipart POST to MinIO with the policy fields. */
function uploadFile(upload: UploadResponse, key: string, content: string) {
  const form = new FormData();
  for (const [k, v] of Object.entries(upload.fields)) form.append(k, v);
  form.set("key", key);
  form.append("file", new Blob([content], { type: "text/csv" }));
  return fetch(upload.url, { method: "POST", body: form });
}

async function uploadCsvFiles(upload: UploadResponse, count: number) {
  for (let i = 0; i < count; i++) {
    const res = await uploadFile(upload, `${upload.prefix}file_${i}.csv`, "0.1,0.2,0.3\n");
    expect(res.status).toBe(204);
  }
}

/** The job once background validation is over (queued, or failed). */
async function afterValidation(jobId: string) {
  let body: { status: string; failure_type: string | null; error: string | null } = {
    status: "validating",
    failure_type: null,
    error: null,
  };
  await expect
    .poll(async () => {
      body = (await request(app).get(`/jobs/${jobId}`).expect(200)).body;
      return body.status;
    })
    .not.toBe("validating");
  return body;
}

beforeAll(async () => {
  await admin.connect();
  await admin.query("LISTEN new_job");
  admin.on("notification", (n) => notifications.push(n.payload ?? ""));
});

afterAll(async () => {
  const { rows } = await admin.query<{ id: string; job_id: string | null }>(
    "SELECT id, job_id FROM uploads WHERE uuid = ANY($1::uuid[])",
    [createdUploads],
  );
  const jobIds = rows.flatMap((r) => (r.job_id ? [r.job_id] : []));
  await admin.query("DELETE FROM uploads WHERE uuid = ANY($1::uuid[])", [createdUploads]);
  await admin.query("DELETE FROM files WHERE job_id = ANY($1::bigint[])", [jobIds]);
  await admin.query("DELETE FROM jobs WHERE id = ANY($1::bigint[])", [jobIds]);
  for (const uuid of createdUploads) {
    const objects = await storage.listPrefix(`inputs/${uuid}/`);
    if (objects.length === 0) continue;
    await s3.send(
      new DeleteObjectsCommand({
        Bucket: config.s3.bucket,
        Delete: { Objects: objects.map((o) => ({ Key: o.key })) },
      }),
    );
  }
  await admin.query("DELETE FROM files WHERE job_id = ANY($1::bigint[])", [insertedJobIds]);
  await admin.query("DELETE FROM jobs WHERE id = ANY($1::bigint[])", [insertedJobIds]);
  await admin.end();
  await pool.end();
});

describe("upload → commit → status", () => {
  it("runs the whole three-step flow", async () => {
    const upload = await openUpload();
    expect(upload.prefix).toBe(`inputs/${upload.upload_id}/`);
    expect(upload.fields).toHaveProperty("Policy");

    await uploadCsvFiles(upload, 3);

    const created = await request(app)
      .post("/jobs")
      .send({ upload_id: upload.upload_id, no_of_files: 3, no_of_columns: 3 })
      .expect(202);
    expect(created.body.status).toBe("validating");
    expect(created.headers.location).toBe(`/jobs/${created.body.id}`);
    expect((await afterValidation(created.body.id)).status).toBe("queued");

    const job = await request(app).get(`/jobs/${created.body.id}`).expect(200);
    expect(job.body).toMatchObject({
      id: created.body.id,
      status: "queued",
      no_of_files: 3,
      no_of_columns: 3,
      progress: { files_done: 0, percent: 0 },
      tasks: { pending: 0, done: 0 },
    });

    const { rows } = await admin.query(
      `SELECT f.type, f.key, f.no_of_files, u.status AS upload_status, j.id::text AS job_id
         FROM jobs j JOIN files f ON f.job_id = j.id JOIN uploads u ON u.job_id = j.id
        WHERE j.uuid = $1 ORDER BY f.key`,
      [created.body.id],
    );
    expect(rows.map((r) => r.key)).toEqual([0, 1, 2].map((i) => `${upload.prefix}file_${i}.csv`));
    expect(rows.every((r) => r.type === "input" && r.no_of_files === 1)).toBe(true);
    expect(rows[0].upload_status).toBe("committed");

    // NOTIFY new_job is delivered after commit, with the internal job id
    await expect.poll(() => notifications).toContain(rows[0].job_id);
  });
});

describe("upload policy", () => {
  it("rejects a key outside the upload's prefix", async () => {
    const upload = await openUpload();
    const res = await uploadFile(upload, "inputs/someone-else/x.csv", "1,2\n");
    expect(res.status).toBe(403);
  });
});

describe("POST /jobs errors", () => {
  it("returns at once; a wrong file count fails the job in validation", async () => {
    const upload = await openUpload();
    await uploadCsvFiles(upload, 2);
    const created = await request(app)
      .post("/jobs")
      .send({ upload_id: upload.upload_id, no_of_files: 3, no_of_columns: 3 })
      .expect(202);
    expect(await afterValidation(created.body.id)).toMatchObject({
      status: "failed",
      failure_type: "user_input",
      error: "expected 3 files, found 2",
    });
    // Nothing reached the orchestrator: no input rows, no NOTIFY
    const { rows } = await admin.query(
      "SELECT count(f.id)::int AS n, j.id::text AS job_id FROM jobs j " +
        "LEFT JOIN files f ON f.job_id = j.id WHERE j.uuid = $1 GROUP BY j.id",
      [created.body.id],
    );
    expect(rows[0].n).toBe(0);
    expect(notifications).not.toContain(rows[0].job_id);
  });

  it("fails validation for a file too small for C values, or not a .csv", async () => {
    const upload = await openUpload();
    await uploadCsvFiles(upload, 1);
    expect((await uploadFile(upload, `${upload.prefix}short.csv`, "1,2\n")).status).toBe(204);
    expect((await uploadFile(upload, `${upload.prefix}notes.txt`, "1,2,3\n")).status).toBe(204);
    const created = await request(app)
      .post("/jobs")
      .send({ upload_id: upload.upload_id, no_of_files: 3, no_of_columns: 3 })
      .expect(202);
    expect(await afterValidation(created.body.id)).toMatchObject({
      status: "failed",
      failure_type: "user_input",
      error: "notes.txt is not a .csv file; short.csv (4 bytes) can't hold 3 values",
    });
  });

  it("409 when the same upload is committed twice", async () => {
    const upload = await openUpload();
    await uploadCsvFiles(upload, 2);
    const body = { upload_id: upload.upload_id, no_of_files: 2, no_of_columns: 3 };
    await request(app).post("/jobs").send(body).expect(202);
    const res = await request(app).post("/jobs").send(body).expect(409);
    expect(res.body.error.message).toBe("upload is already committed");
  });

  it("409 when the upload has expired", async () => {
    const upload = await openUpload();
    await uploadCsvFiles(upload, 2);
    await admin.query(
      "UPDATE uploads SET expires_at = now() - interval '1 second' WHERE uuid = $1",
      [upload.upload_id],
    );
    const res = await request(app)
      .post("/jobs")
      .send({ upload_id: upload.upload_id, no_of_files: 2, no_of_columns: 3 })
      .expect(409);
    expect(res.body.error.message).toBe("upload has expired");
  });

  it("404 for an unknown upload", async () => {
    await request(app)
      .post("/jobs")
      .send({ upload_id: crypto.randomUUID(), no_of_files: 2, no_of_columns: 3 })
      .expect(404);
  });

  it.each([
    [{ no_of_files: 1, no_of_columns: 3 }, "F must be > 1"],
    [{ no_of_files: 100_000, no_of_columns: 3 }, "F must be < 100k"],
    [{ no_of_files: 2, no_of_columns: 1_000_000 }, "C must be < 1000k"],
    [{ no_of_files: 2.5, no_of_columns: 3 }, "F must be an integer"],
  ])("400 for out-of-range input %o (%s)", async (fields) => {
    const res = await request(app)
      .post("/jobs")
      .send({ upload_id: crypto.randomUUID(), ...fields })
      .expect(400);
    expect(res.body.error.code).toBe("validation_error");
  });

  it("400 for malformed JSON", async () => {
    await request(app)
      .post("/jobs")
      .set("content-type", "application/json")
      .send("{not json")
      .expect(400);
  });
});

describe("validation lease", () => {
  /** A committed upload with its job left validating, as a stopped API would leave it. */
  async function strandedJob(leaseOffsetSecs: number) {
    const upload = await openUpload();
    await uploadCsvFiles(upload, 2);
    const { rows } = await admin.query<{ id: string; uuid: string }>(
      `INSERT INTO jobs (no_of_files, no_of_columns, validating_until)
       VALUES (2, 3, now() + make_interval(secs => $1)) RETURNING id, uuid`,
      [leaseOffsetSecs],
    );
    const job = rows[0];
    if (!job) throw new Error("insert failed");
    await admin.query("UPDATE uploads SET status = 'committed', job_id = $2 WHERE uuid = $1", [
      upload.upload_id,
      job.id,
    ]);
    return job;
  }

  const statusOf = async (uuid: string) =>
    (await request(app).get(`/jobs/${uuid}`).expect(200)).body.status;

  it("leaves a job alone while another replica's lease is live", async () => {
    const job = await strandedJob(3600);
    await validateJob(deps, job.id);
    expect(await statusOf(job.uuid)).toBe("validating");
  });

  it("takes over once the lease has lapsed, and queues the job once", async () => {
    const job = await strandedJob(-1);
    await Promise.all([validateJob(deps, job.id), validateJob(deps, job.id)]);
    expect(await statusOf(job.uuid)).toBe("queued");
    const { rows } = await admin.query("SELECT count(*)::int AS n FROM files WHERE job_id = $1", [
      job.id,
    ]);
    expect(rows[0].n).toBe(2);
  });
});

describe("problemWith", () => {
  const job = { id: "1", no_of_files: 5, no_of_columns: 3, upload_uuid: "u" };
  const file = (name: string, size = 12) => ({ key: `p/${name}`, size });

  it("accepts files whose sizes can hold C values", () => {
    const files = ["a", "b", "c", "d", "e"].map((n) => file(`${n}.CSV`));
    expect(problemWith(job, "p/", files)).toBeNull();
  });

  it("names the first few problems and counts the rest", () => {
    const files = ["a", "b", "c", "d", "e"].map((n) => file(`${n}.csv`, 1));
    expect(problemWith(job, "p/", files)).toBe(
      "a.csv (1 bytes) can't hold 3 values; b.csv (1 bytes) can't hold 3 values; " +
        "c.csv (1 bytes) can't hold 3 values and 2 more",
    );
  });

  it("rejects files in sub-folders", () => {
    const files = [file("x/a.csv"), ...["b", "c", "d", "e"].map((n) => file(`${n}.csv`))];
    expect(problemWith(job, "p/", files)).toBe("x/a.csv is in a sub-folder");
  });
});

describe("GET /jobs/:id", () => {
  it("400 for a non-uuid id", async () => {
    await request(app).get("/jobs/123").expect(400);
  });

  it("404 for an unknown job", async () => {
    await request(app).get(`/jobs/${crypto.randomUUID()}`).expect(404);
  });
});

describe("DTOs", () => {
  it("rejects unknown request fields", async () => {
    const res = await request(app)
      .post("/jobs")
      .send({ upload_id: crypto.randomUUID(), no_of_files: 2, no_of_columns: 3, priority: 9 })
      .expect(400);
    expect(res.body.error.code).toBe("validation_error");
    await request(app).post("/uploads").send({ ttl: 999 }).expect(400);
  });

  it("returns exactly the response DTO fields: no internal ids, ISO dates", async () => {
    const upload = await openUpload();
    expect(Object.keys(upload).sort()).toEqual(
      ["expires_at", "fields", "max_file_bytes", "prefix", "upload_id", "url"].sort(),
    );
    await uploadCsvFiles(upload, 2);
    const created = await request(app)
      .post("/jobs")
      .send({ upload_id: upload.upload_id, no_of_files: 2, no_of_columns: 3 })
      .expect(202);
    expect(Object.keys(created.body).sort()).toEqual(["id", "status"]);

    const job = await request(app).get(`/jobs/${created.body.id}`).expect(200);
    expect(Object.keys(job.body).sort()).toEqual(
      [
        "id",
        "status",
        "no_of_files",
        "no_of_columns",
        "files_per_task",
        "progress",
        "tasks",
        "failure_type",
        "error",
        "created_at",
        "started_at",
        "completed_at",
      ].sort(),
    );
    expect(job.body.id).toMatch(/^[0-9a-f-]{36}$/); // the uuid, never the bigint
    expect(new Date(job.body.created_at).toISOString()).toBe(job.body.created_at);
  });
});

/** A job in a given state, as the orchestrator would leave it. */
async function insertJob(status: string): Promise<{ id: string; uuid: string }> {
  const { rows } = await admin.query<{ id: string; uuid: string }>(
    `INSERT INTO jobs (no_of_files, no_of_columns, status, failure_type, completed_at)
     VALUES (4, 3, $1, $2, CASE WHEN $1 IN ('succeeded', 'failed') THEN now() END)
     RETURNING id, uuid`,
    [status, status === "failed" ? "internal" : null],
  );
  const job = rows[0];
  if (!job) throw new Error("insert failed");
  insertedJobIds.push(job.id);
  return job;
}

describe("GET /jobs", () => {
  it("lists newest first with a cursor and a status filter", async () => {
    const a = await insertJob("succeeded");
    const b = await insertJob("running");
    const c = await insertJob("failed");

    const page1 = await request(app).get("/jobs?limit=2").expect(200);
    expect(page1.body.jobs.map((j: { id: string }) => j.id)).toEqual([c.uuid, b.uuid]);
    expect(page1.body.next).toBe(b.uuid);
    expect(page1.body.jobs[0]).toMatchObject({
      status: "failed",
      failure_type: "internal",
      percent: 0,
    });

    const page2 = await request(app).get(`/jobs?limit=2&before=${page1.body.next}`).expect(200);
    expect(page2.body.jobs[0].id).toBe(a.uuid);

    const running = await request(app).get("/jobs?status=running&limit=200").expect(200);
    const ids = running.body.jobs.map((j: { id: string }) => j.id);
    expect(ids).toContain(b.uuid);
    expect(ids).not.toContain(a.uuid);
  });

  it("rejects a bad query", async () => {
    await request(app).get("/jobs?status=paused").expect(400);
    await request(app).get("/jobs?limit=1000").expect(400);
    await request(app).get("/jobs?cursor=x").expect(400);
  });
});

describe("GET /jobs/:id/result", () => {
  it("409 until the job has succeeded", async () => {
    const job = await insertJob("running");
    const res = await request(app).get(`/jobs/${job.uuid}/result`).expect(409);
    expect(res.body.error.message).toMatch(/job is running/);
  });

  it("404 for an unknown job", async () => {
    await request(app).get(`/jobs/${crypto.randomUUID()}/result`).expect(404);
  });

  it("returns a presigned link that downloads the result CSV", async () => {
    const job = await insertJob("succeeded");
    const key = `results/${job.uuid}.csv`;
    await s3.send(
      new PutObjectCommand({ Bucket: config.s3.bucket, Key: key, Body: "2.5,3.5,4.5\n" }),
    );
    // the final task that produced the result (the schema requires a producer)
    const { rows } = await admin.query<{ id: string }>(
      `INSERT INTO tasks (job_id, kind, no_of_files, is_final, status)
       VALUES ($1, 'map', 4, true, 'done') RETURNING id`,
      [job.id],
    );
    await admin.query(
      `INSERT INTO files (job_id, type, key, no_of_files, produced_by)
       VALUES ($1, 'result', $2, 4, $3)`,
      [job.id, key, rows[0]?.id],
    );

    const res = await request(app).get(`/jobs/${job.uuid}/result`).expect(200);
    expect(Object.keys(res.body).sort()).toEqual(["expires_at", "url"]);
    const download = await fetch(res.body.url);
    expect(download.status).toBe(200);
    expect(await download.text()).toBe("2.5,3.5,4.5\n");

    await admin.query("DELETE FROM files WHERE job_id = $1", [job.id]);
    await admin.query("DELETE FROM tasks WHERE job_id = $1", [job.id]);
    await s3.send(
      new DeleteObjectsCommand({ Bucket: config.s3.bucket, Delete: { Objects: [{ Key: key }] } }),
    );
  });
});

describe("/workers", () => {
  it("combines W from Postgres with live workers from the orchestrator", async () => {
    const job = await insertJob("running");
    workersSnapshot = {
      readyTasks: 7,
      workers: [
        {
          workerId: "w-busy",
          busy: true,
          taskId: "42",
          jobId: job.id,
          kind: "map",
          attempt: 2,
          secondsOnTask: 3.14,
          secondsSinceSeen: 1.2,
        },
        {
          workerId: "w-idle",
          busy: false,
          taskId: "0",
          jobId: "0",
          kind: "",
          attempt: 0,
          secondsOnTask: 0,
          secondsSinceSeen: 0.4,
        },
      ],
    };
    const res = await request(app).get("/workers").expect(200);
    expect(res.body).toMatchObject({ live: 2, busy: 1, idle: 1, ready_tasks: 7 });
    expect(res.body.workers[0]).toEqual({
      id: "w-busy",
      status: "busy",
      task: { id: "42", job_id: job.uuid, kind: "map", attempt: 2, seconds: 3.1 },
      seconds_since_seen: 1.2,
    });
    expect(res.body.workers[1]).toMatchObject({ id: "w-idle", status: "idle", task: null });
  });

  it("503 while the orchestrator is unreachable", async () => {
    workersSnapshot = new AppError(503, "orchestrator_unavailable", "orchestrator: down");
    const res = await request(app).get("/workers").expect(503);
    expect(res.body.error.code).toBe("orchestrator_unavailable");
    workersSnapshot = { workers: [], readyTasks: 0 };
  });

  it("PUT sets the desired number of workers and notifies the supervisor", async () => {
    const listener = new pg.Client({
      host: config.db.host,
      port: config.db.port,
      database: config.db.database,
      user: process.env.POSTGRES_USER,
      password: process.env.POSTGRES_PASSWORD,
    });
    await listener.connect();
    const heard: string[] = [];
    await listener.query("LISTEN settings_changed");
    listener.on("notification", (n) => heard.push(n.payload ?? ""));
    const before = (await request(app).get("/workers").expect(200)).body.desired_workers;
    try {
      await request(app)
        .put("/workers")
        .send({ desired_workers: 7 })
        .expect(200, { desired_workers: 7 });
      expect((await request(app).get("/workers")).body.desired_workers).toBe(7);
      await expect.poll(() => heard).toContain("7");
      await request(app).put("/workers").send({ desired_workers: 10_001 }).expect(400);
      await request(app).put("/workers").send({ desired_workers: 3, extra: 1 }).expect(400);
    } finally {
      await request(app).put("/workers").send({ desired_workers: before }).expect(200);
      await listener.end();
    }
  });
});

describe("GET /stats", () => {
  it("counts jobs by state and tasks of active jobs", async () => {
    const before = (await request(app).get("/stats").expect(200)).body;
    const job = await insertJob("running");
    await admin.query(
      `INSERT INTO tasks (job_id, kind, no_of_files, status) VALUES ($1, 'map', 2, 'pending'),
              ($1, 'map', 2, 'running')`,
      [job.id],
    );
    const after = (await request(app).get("/stats").expect(200)).body;
    expect(after.jobs.running).toBe(before.jobs.running + 1);
    expect(after.tasks_of_active_jobs.pending).toBe(before.tasks_of_active_jobs.pending + 1);
    expect(after.tasks_of_active_jobs.running).toBe(before.tasks_of_active_jobs.running + 1);
    expect(Object.keys(after).sort()).toEqual(["jobs", "last_minute", "tasks_of_active_jobs"]);
    await admin.query("DELETE FROM tasks WHERE job_id = $1", [job.id]);
  });
});

describe("GET /events (SSE)", () => {
  it("streams a workers snapshot and logs what changed", async () => {
    workersSnapshot = {
      readyTasks: 0,
      workers: [
        {
          workerId: "w1",
          busy: false,
          taskId: "0",
          jobId: "0",
          kind: "",
          attempt: 0,
          secondsOnTask: 0,
          secondsSinceSeen: 0.1,
        },
      ],
    };
    const server = app.listen(0);
    const port = (server.address() as { port: number }).port;
    const controller = new AbortController();
    try {
      const res = await fetch(`http://127.0.0.1:${port}/events`, { signal: controller.signal });
      expect(res.headers.get("content-type")).toMatch(/^text\/event-stream/);
      if (!res.body) throw new Error("no stream");
      const reader = res.body.getReader();
      const decoder = new TextDecoder();
      let text = "";
      const readUntil = async (needle: string) => {
        while (!text.includes(needle)) {
          const { value } = await reader.read();
          text += decoder.decode(value);
        }
      };
      await readUntil("event: workers");
      expect(text).toContain('"live":1');
      // a second worker joins and the first starts a retried task
      workersSnapshot = {
        readyTasks: 0,
        workers: [
          {
            workerId: "w1",
            busy: true,
            taskId: "7",
            jobId: "0",
            kind: "merge",
            attempt: 2,
            secondsOnTask: 1,
            secondsSinceSeen: 0.1,
          },
          {
            workerId: "w2",
            busy: false,
            taskId: "0",
            jobId: "0",
            kind: "",
            attempt: 0,
            secondsOnTask: 0,
            secondsSinceSeen: 0.1,
          },
        ],
      };
      await readUntil("worker w2 joined");
      await readUntil("worker w1 started merge task 7 (attempt 2)");
    } finally {
      controller.abort();
      server.close();
      workersSnapshot = { workers: [], readyTasks: 0 };
    }
  });
});
