import { dirname, resolve } from "node:path";
import { fileURLToPath } from "node:url";
import * as grpc from "@grpc/grpc-js";
import * as protoLoader from "@grpc/proto-loader";
import { AppError } from "../lib/errors.js";

// proto/ sits next to src/ and dist/: two levels up from this file in both
const PROTO = resolve(dirname(fileURLToPath(import.meta.url)), "../../proto/admin.proto");

export interface WorkerInfo {
  workerId: string;
  busy: boolean;
  taskId: string; // int64 as string
  jobId: string; // internal job id, int64 as string
  kind: string;
  attempt: number;
  secondsOnTask: number;
  secondsSinceSeen: number;
}

export interface WorkersSnapshot {
  workers: WorkerInfo[];
  readyTasks: number;
}

export interface OrchestratorClient {
  listWorkers(): Promise<WorkersSnapshot>;
  close(): void;
}

type AdminStub = grpc.Client & {
  ListWorkers(
    request: object,
    options: grpc.CallOptions,
    callback: (err: grpc.ServiceError | null, response: WorkersSnapshot) => void,
  ): void;
};

/** Read-only calls to the orchestrator's Admin gRPC service (live worker state). */
export function createOrchestratorClient(address: string): OrchestratorClient {
  const definition = protoLoader.loadSync(PROTO, {
    longs: String,
    defaults: true,
  });
  const loaded = grpc.loadPackageDefinition(definition) as unknown as {
    dmean: { v1: { Admin: new (addr: string, creds: grpc.ChannelCredentials) => AdminStub } };
  };
  const stub = new loaded.dmean.v1.Admin(address, grpc.credentials.createInsecure());

  return {
    listWorkers: () =>
      new Promise((resolvePromise, reject) => {
        stub.ListWorkers({}, { deadline: Date.now() + 3000 }, (err, response) => {
          if (err) {
            reject(
              new AppError(
                503,
                "orchestrator_unavailable",
                `orchestrator: ${err.details || err.message}`,
              ),
            );
            return;
          }
          resolvePromise(response);
        });
      }),
    close: () => stub.close(),
  };
}
