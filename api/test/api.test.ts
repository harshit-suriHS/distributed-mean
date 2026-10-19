// Integration tests: need `docker compose up` (Postgres, MinIO, migrations applied).
import { DeleteObjectsCommand, S3Client } from "@aws-sdk/client-s3";
import pg from "pg";
import request from "supertest";
import { afterAll, beforeAll, describe, expect, it } from "vitest";
import { createApp } from "../src/app.js";
import { loadConfig } from "../src/config.js";
import { createPool } from "../src/db/pool.js";
import { createLogger } from "../src/lib/logger.js";
import { createStorage } from "../src/storage/s3.js";

const config = loadConfig();
const pool = createPool(config.db);
const storage = createStorage(config.s3);
const app = createApp({ config, pool, storage, logger: createLogger("silent") });

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
    expect(created.body.status).toBe("queued");
    expect(created.headers.location).toBe(`/jobs/${created.body.id}`);

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
  it("400 when the file count does not match, and the upload stays open", async () => {
    const upload = await openUpload();
    await uploadCsvFiles(upload, 2);
    const res = await request(app)
      .post("/jobs")
      .send({ upload_id: upload.upload_id, no_of_files: 3, no_of_columns: 3 })
      .expect(400);
    expect(res.body.error).toEqual({
      code: "file_count_mismatch",
      message: "expected 3 files, found 2",
    });
    const { rows } = await admin.query("SELECT status FROM uploads WHERE uuid = $1", [
      upload.upload_id,
    ]);
    expect(rows[0].status).toBe("open");
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
