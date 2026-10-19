// The real gRPC client against an in-process fake Admin server (no database needed).
import { dirname, resolve } from "node:path";
import { fileURLToPath } from "node:url";
import * as grpc from "@grpc/grpc-js";
import * as protoLoader from "@grpc/proto-loader";
import { afterAll, beforeAll, describe, expect, it } from "vitest";
import { createOrchestratorClient } from "../src/orchestrator/client.js";

const PROTO = resolve(dirname(fileURLToPath(import.meta.url)), "../proto/admin.proto");
let server: grpc.Server;
let address = "";

beforeAll(async () => {
  const pkg = grpc.loadPackageDefinition(protoLoader.loadSync(PROTO)) as unknown as {
    dmean: { v1: { Admin: { service: grpc.ServiceDefinition } } };
  };
  server = new grpc.Server();
  server.addService(pkg.dmean.v1.Admin.service, {
    ListWorkers: (_call: unknown, callback: grpc.sendUnaryData<unknown>) =>
      callback(null, {
        readyTasks: 3,
        workers: [
          {
            workerId: "w1",
            busy: true,
            taskId: "9007199254740993",
            jobId: "5",
            kind: "merge",
            attempt: 1,
            secondsOnTask: 2.5,
            secondsSinceSeen: 0.1,
          },
        ],
      }),
  });
  const port = await new Promise<number>((ok, fail) =>
    server.bindAsync("127.0.0.1:0", grpc.ServerCredentials.createInsecure(), (err, p) =>
      err ? fail(err) : ok(p),
    ),
  );
  address = `127.0.0.1:${port}`;
});

afterAll(() => server.forceShutdown());

describe("orchestrator client", () => {
  it("maps ListWorkers, keeping int64 ids exact as strings", async () => {
    const client = createOrchestratorClient(address);
    const snap = await client.listWorkers();
    expect(snap.readyTasks).toBe(3);
    expect(snap.workers[0]).toMatchObject({
      workerId: "w1",
      busy: true,
      kind: "merge",
      jobId: "5",
    });
    expect(snap.workers[0]?.taskId).toBe("9007199254740993"); // > 2^53: would be lossy as a number
    client.close();
  });

  it("reports an unreachable orchestrator as 503", async () => {
    const client = createOrchestratorClient("127.0.0.1:1");
    await expect(client.listWorkers()).rejects.toMatchObject({
      status: 503,
      code: "orchestrator_unavailable",
    });
    client.close();
  });
});
