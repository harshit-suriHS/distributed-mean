import { z } from "zod";

const Env = z.object({
  API_PORT: z.coerce.number().int().default(3000),
  LOG_LEVEL: z.string().default("info"),

  POSTGRES_HOST: z.string().default("localhost"),
  POSTGRES_PORT: z.coerce.number().int().default(5432),
  POSTGRES_DB: z.string(),
  API_DB_PASSWORD: z.string(),

  S3_ENDPOINT: z.url(),
  // Host that clients use for presigned uploads; differs from S3_ENDPOINT when the API runs in Docker
  S3_PUBLIC_ENDPOINT: z.url().optional(),
  S3_REGION: z.string().default("us-east-1"),
  S3_ACCESS_KEY: z.string(),
  S3_SECRET_KEY: z.string(),
  S3_BUCKET: z.string(),

  ORCHESTRATOR_ADDR: z.string().default("localhost:50051"),

  UPLOAD_TTL_HOURS: z.coerce.number().positive().default(24),
  MAX_UPLOAD_FILE_BYTES: z.coerce
    .number()
    .int()
    .positive()
    .default(32 * 1024 * 1024),
});

export function loadConfig(env: NodeJS.ProcessEnv = process.env) {
  const e = Env.parse(env);
  return {
    port: e.API_PORT,
    logLevel: e.LOG_LEVEL,
    db: {
      host: e.POSTGRES_HOST,
      port: e.POSTGRES_PORT,
      database: e.POSTGRES_DB,
      user: "api",
      password: e.API_DB_PASSWORD,
    },
    s3: {
      endpoint: e.S3_ENDPOINT,
      publicEndpoint: e.S3_PUBLIC_ENDPOINT ?? e.S3_ENDPOINT,
      region: e.S3_REGION,
      accessKey: e.S3_ACCESS_KEY,
      secretKey: e.S3_SECRET_KEY,
      bucket: e.S3_BUCKET,
    },
    orchestratorAddr: e.ORCHESTRATOR_ADDR,
    uploads: {
      ttlHours: e.UPLOAD_TTL_HOURS,
      maxFileBytes: e.MAX_UPLOAD_FILE_BYTES,
    },
  };
}

export type Config = ReturnType<typeof loadConfig>;
