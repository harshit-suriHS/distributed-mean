# Distributed Mean

Computes the per-index mean of `F` CSV files (each with `C` numbers) across a fleet of `W` workers.

- **API:** Node, Express, TypeScript
- **DB:** Postgres
- **Queue:** a fair, lease-based task queue in the orchestrator, served over gRPC and durable in Postgres
- **Workers:** Python

The full design, with trade-offs and a decision log, is in [`design.md`](design.md). The brief is in [`problem_statement.md`](problem_statement.md).

## How it works

```
user ──upload CSVs (presigned policy)──► MinIO (S3)
     ──POST /jobs──► API ──insert job + NOTIFY──► Postgres ◄──── every state change ────┐
                                                     │                                 │
                                          LISTEN new_job                               │
                                                     ▼                                 │
           worker ──gRPC Lease (long-poll)──► Orchestrator ── fair queue + leases ─────┘
           worker ◄── task: input keys, C, output key
           worker ──read CSVs / write partial──► MinIO
           worker ──Report(done)──► Orchestrator ──► merges until one task covers all F files
```

1. **Upload, then commit.**
   - `POST /uploads` returns one presigned POST policy for a whole prefix, and the client uploads its CSVs in parallel.
   - `POST /jobs` checks the files are all there and creates the job.
2. **Plan.** The orchestrator splits the job into map tasks of N files each. N is sized from F, C and the number of live workers, so tasks take about 25 s.
3. **Pull with leases.**
   - A free worker long-polls `Lease`, and the orchestrator picks a task at that moment.
   - It picks merges first; otherwise it takes the job with the fewest tasks in flight (max-min fairness, the same rule as the Hadoop Fair Scheduler and SQS fair queues).
   - A lease lasts 30 s and is renewed by heartbeats. If the worker dies, the lease expires and another worker retries the task.
4. **Partials and merges.**
   - Each map writes `(sum vector, count)`.
   - Full groups of partials are merged as they arrive (a dynamic pool, not a fixed tree), so nothing waits for a slow worker.
   - The task covering all F files writes the mean.
5. **Postgres is the source of truth.**
   - Every decision is committed before a worker hears about it, and `UNIQUE (file_id)` makes double counting impossible.
   - A restarted orchestrator rebuilds its state from the database.

## Quick start

Requires Docker and [uv](https://docs.astral.sh/uv/).

```sh
cp .env.example .env
docker compose up -d --build        # postgres, minio, migrations, api, orchestrator, supervisor (4 workers), ui
open http://localhost:8080          # the dashboard: submit jobs, scale W, watch workers and the live log
uv run scripts/demo.py --files 50 --columns 1000    # or from the command line
```

```sh
uv run scripts/demo.py --values "1,2,3" "4,5,6"     # the brief's example → [2.5, 3.5, 4.5]
curl -X PUT localhost:3000/workers -H 'content-type: application/json' -d '{"desired_workers": 8}'   # the supervisor follows within seconds
docker compose logs -f orchestrator supervisor      # JSON logs with job_id / task_id / worker_id
```

With production sizing a task is ~25 s of work, so small demo jobs fit in a single task. To watch maps and merges on a small job, set `MAX_FILES_PER_TASK=10` in `.env` and restart the orchestrator (`docker compose up -d orchestrator`).

Service URLs and ports:
- UI: http://localhost:8080
- API: http://localhost:3000
- MinIO console: http://localhost:9001 (login `minioadmin` / `minioadmin`)
- Orchestrator gRPC: `localhost:50051`

## API

| Method | Path | |
|---|---|---|
| `POST` | `/uploads` | open an upload, returns a presigned POST policy for `inputs/{upload_id}/` |
| `POST` | `/jobs` | `{upload_id, no_of_files, no_of_columns}` → `202 {id, status}` |
| `GET` | `/jobs/:id` | status, progress, task counts, failure reason |
| `GET` | `/jobs` | list jobs (`?status=`) |
| `GET` | `/jobs/:id/result` | presigned download URL for the result CSV |
| `GET` / `PUT` | `/workers` | live workers / set the desired number of workers |
| `GET` | `/stats` | jobs and tasks by state |
| `GET` | `/events` | server-sent events: live worker snapshots and log lines (used by the UI) |

Files are CSV: an input is one line of `C` comma-separated numbers, and so is the result. Bad input (a non-number, the wrong number of values, NaN) fails the job with `failure_type: user_input` and the file name in the error.

## Repository layout

| Path | What |
|---|---|
| `api/` | Node/Express/TS public API: uploads, jobs. Layers: routes → service → repo, zod DTOs |
| `orchestrator/` | Python: planner, fair lease-based queue (gRPC server), merge pool, recovery, storage cleanup |
| `worker/` | Python: lease → sum the CSVs (streaming, NumPy) → write the partial or result → report |
| `supervisor/` | Python: keeps `desired_workers` worker processes running (spawn, drain, replace hung ones) |
| `ui/` | Vite + React dashboard: stats, workers + W control, jobs, live log, new job |
| `db/` | dbmate migrations and the role setup script |
| `e2e/` | end-to-end tests that run every service as a real process |
| `scripts/demo.py` | try it as a user |
| `docker-compose.yml` | the whole system |

## Configuration

Everything is in `.env` (see `.env.example`).

| Variable | Default | Meaning |
|---|---|---|
| `UPLOAD_TTL_HOURS` | 24 | uncommitted uploads expire and are deleted |
| `MAX_UPLOAD_FILE_BYTES` | 32 MiB | per-file upload limit |
| `TARGET_TASK_SECS`, `MAX_FILES_PER_TASK` | 25, 5000 | task sizing (orchestrator) |
| `LEASE_SECS`, `HEARTBEAT_SECS` | 30, 10 | lease length and renewal (orchestrator) |
| `SLOWDOWN_SECS_PER_FILE` | 0 | make a worker artificially slow, for demos of heterogeneous speed |

## Development and tests

Run the infrastructure in Docker and the services from source:

```sh
docker compose up -d postgres minio minio-init db-roles migrate
(cd api && npm install && npm run dev)
(cd orchestrator && uv sync && uv run dmean-orchestrator)
(cd worker && uv sync && uv run dmean-worker)          # or: (cd supervisor && uv sync && uv run dmean-supervisor)
```

Tests (stop the dockerised api/orchestrator/supervisor first, so they don't pick up test jobs: `docker compose stop api orchestrator supervisor`):

| Suite | Command | Covers |
|---|---|---|
| API | `cd api && npm test` | uploads, commit, DTOs, against real Postgres + MinIO |
| Orchestrator | `cd orchestrator && uv run pytest` | fair queue, sizing, merge pool, gRPC service, restart recovery, cleanup |
| Worker | `cd worker && uv run pytest` | CSV format, summing, the worker loop against a fake orchestrator |
| Supervisor | `cd supervisor && uv run pytest` | scaling decisions, spawn / drain / replace, following `PUT /workers` via NOTIFY |
| End to end | `cd e2e && uv run pytest` | real processes: correct means, concurrent jobs, killed worker, orchestrator restart, slow worker |
| Load | `cd orchestrator && uv run python scripts/load_test.py --workers 500` | 500–1K simulated workers on the real protocol |

Lint and types: `npx biome check` (api), `uv run ruff check && uv run pyright` (Python, strict).

## Results

- **Correctness:** results match `numpy.mean` to ~1e-14. CSV floats round-trip bit-for-bit.
- **Failures:** a worker killed mid-task has its task retried elsewhere, and an orchestrator restart resumes the job. A bad input file fails the job with a clear reason.
- **Load** (laptop):
  - 500 simulated workers: 444 tasks/s
  - 1K simulated workers: 554 tasks/s, about 14× the real load at 1K workers with 25 s tasks
  - Tasks done per worker were proportional to its speed.

## Not done yet

- MinIO per-service users and policies; the stack uses the root key everywhere (design §20).
- Tail splitting, measured worker speed, per-job priorities (design §26).
