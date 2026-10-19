"""
Load test: N simulated workers against a real orchestrator process (real gRPC, real Postgres).

Simulated workers speak the real protocol (Lease long-poll → Heartbeat → Report) but don't read
or write files: they sleep for their "work time", with speeds spread 1×–5× (heterogeneous
workers). Jobs are inserted straight into Postgres as the API would leave them.

    uv run python scripts/load_test.py --workers 500 --jobs 20 --files 2000

Needs `docker compose up` (postgres). Cleans up its rows afterwards.
"""

import argparse
import asyncio
import os
import random
import statistics
import subprocess
import sys
import time
import uuid
from pathlib import Path
from typing import Any

import grpc
import psycopg

from dmean_orchestrator.rpc import task_queue_pb2 as pb
from dmean_orchestrator.rpc import task_queue_pb2_grpc as pb_grpc

ROOT = Path(__file__).resolve().parents[2]


def read_env() -> dict[str, str]:
    env: dict[str, str] = {}
    for line in (ROOT / ".env").read_text().splitlines():
        line = line.strip()
        if line and not line.startswith("#") and "=" in line:
            key, value = line.split("=", 1)
            env[key] = value
    return env


ENV = read_env()


class Stats:
    def __init__(self) -> None:
        self.lease_ms: list[float] = []  # leases that returned a task
        self.report_ms: list[float] = []
        self.done = 0
        self.stopped = 0  # tasks abandoned on STOP
        self.by_speed: dict[str, int] = {}
        self.spread: list[int] = []  # max - min tasks in flight across active jobs


async def worker(
    stub: pb_grpc.TaskQueueStub,
    worker_id: str,
    speed: float,
    base_secs: float,
    stats: Stats,
    stop: asyncio.Event,
) -> None:
    label = f"{speed:.0f}x"
    while not stop.is_set():
        started = time.perf_counter()
        try:
            resp = await stub.Lease(
                pb.LeaseRequest(worker_id=worker_id, wait_seconds=2), timeout=10
            )
        except grpc.aio.AioRpcError:
            await asyncio.sleep(0.5)
            continue
        if not resp.HasField("task"):
            continue
        stats.lease_ms.append((time.perf_counter() - started) * 1000)
        task = resp.task
        # a task reads len(input_keys) files (inputs or partials); slower workers take longer
        work = base_secs * speed * (len(task.input_keys) / 10)
        stopped = False
        while work > 0:
            step = min(work, float(task.heartbeat_seconds))
            await asyncio.sleep(step)
            work -= step
            if work > 0:
                hb = await stub.Heartbeat(
                    pb.HeartbeatRequest(
                        worker_id=worker_id, task_id=task.task_id, attempt=task.attempt
                    )
                )
                if hb.action == pb.HeartbeatResponse.STOP:  # like a real worker: abandon
                    stopped = True
                    break
        if stopped:
            stats.stopped += 1
            continue
        started = time.perf_counter()
        await stub.Report(
            pb.ReportRequest(
                worker_id=worker_id,
                task_id=task.task_id,
                attempt=task.attempt,
                done=pb.Done(output_key=task.output_key, size_bytes=1),
            ),
            timeout=30,
        )
        stats.report_ms.append((time.perf_counter() - started) * 1000)
        stats.done += 1
        stats.by_speed[label] = stats.by_speed.get(label, 0) + 1


def insert_jobs(conn: psycopg.Connection[Any], jobs: int, files: int, run: str) -> list[int]:
    job_ids: list[int] = []
    for j in range(jobs):
        row = conn.execute(
            "INSERT INTO jobs (no_of_files, no_of_columns) VALUES (%s, 1000) RETURNING id", (files,)
        ).fetchone()
        assert row is not None
        job_id = int(row[0])
        job_ids.append(job_id)
        keys = [f"loadtest/{run}/{j}/{i}.csv" for i in range(files)]
        conn.execute(
            """INSERT INTO files (job_id, type, key, no_of_files)
               SELECT %s, 'input', k, 1 FROM unnest(%s::text[]) AS k""",
            (job_id, keys),
        )
    return job_ids


def cleanup(conn: psycopg.Connection[Any], job_ids: list[int]) -> None:
    conn.execute(
        "DELETE FROM task_files WHERE task_id IN (SELECT id FROM tasks WHERE job_id = ANY(%s))",
        (job_ids,),
    )
    conn.execute("DELETE FROM files WHERE job_id = ANY(%s)", (job_ids,))
    conn.execute("DELETE FROM tasks WHERE job_id = ANY(%s)", (job_ids,))
    conn.execute("DELETE FROM jobs WHERE id = ANY(%s)", (job_ids,))


def pct(values: list[float], p: float) -> float:
    if not values:
        return 0.0
    ordered = sorted(values)
    return ordered[min(len(ordered) - 1, int(p / 100 * len(ordered)))]


async def run(args: argparse.Namespace) -> int:
    dsn = (
        f"host=localhost port={ENV['POSTGRES_PORT']} dbname={ENV['POSTGRES_DB']} "
        f"user={ENV['POSTGRES_USER']} password={ENV['POSTGRES_PASSWORD']}"
    )
    port = 50071
    orchestrator = subprocess.Popen(
        [str(ROOT / "orchestrator/.venv/bin/dmean-orchestrator")],
        cwd=ROOT / "orchestrator",
        env={
            **os.environ,
            **ENV,
            "GRPC_PORT": str(port),
            "LOG_LEVEL": "warning",
            "ADVISORY_LOCK_KEY": "4444",
            "MAX_FILES_PER_TASK": str(args.files_per_task),
            "DB_POOL_SIZE": "10",
        },
    )
    await asyncio.sleep(3)
    conn = psycopg.connect(dsn, autocommit=True)
    run_id = uuid.uuid4().hex[:8]
    job_ids: list[int] = []
    stats = Stats()
    stop = asyncio.Event()
    channels = [grpc.aio.insecure_channel(f"127.0.0.1:{port}") for _ in range(10)]
    rng = random.Random(7)
    try:
        # workers first, so they are "live" when jobs are planned (W in the sizing formula)
        tasks = [
            asyncio.create_task(
                worker(
                    pb_grpc.TaskQueueStub(channels[i % len(channels)]),
                    f"sim-{i}",
                    rng.choice([1.0, 2.0, 3.0, 5.0]),
                    args.base_secs,
                    stats,
                    stop,
                )
            )
            for i in range(args.workers)
        ]
        await asyncio.sleep(2)
        job_ids = insert_jobs(conn, args.jobs, args.files, run_id)
        started = time.monotonic()
        for job_id in job_ids:
            conn.execute("SELECT pg_notify('new_job', %s)", (str(job_id),))

        while time.monotonic() - started < args.timeout:
            await asyncio.sleep(1)
            rows = conn.execute(
                """SELECT job_id, count(*) FILTER (WHERE status = 'running'),
                          count(*) FILTER (WHERE status = 'pending')
                     FROM tasks WHERE job_id = ANY(%s) GROUP BY job_id""",
                (job_ids,),
            ).fetchall()
            active = [int(r[1]) for r in rows if int(r[2]) > 0]  # jobs still with work queued
            if len(active) >= 2:
                stats.spread.append(max(active) - min(active))
            finished = conn.execute(
                "SELECT count(*) FROM jobs WHERE id = ANY(%s) AND status = 'succeeded'", (job_ids,)
            ).fetchone()
            if finished is not None and int(finished[0]) == len(job_ids):
                break
        elapsed = time.monotonic() - started
        ps = subprocess.run(
            ["ps", "-o", "%cpu=,rss=", "-p", str(orchestrator.pid)], capture_output=True, text=True
        ).stdout.split()
        succeeded = conn.execute(
            "SELECT count(*) FROM jobs WHERE id = ANY(%s) AND status = 'succeeded'", (job_ids,)
        ).fetchone()
        totals = conn.execute(
            """SELECT count(*) FILTER (WHERE kind = 'map'), count(*) FILTER (WHERE kind = 'merge'),
                      max(attempts) FROM tasks WHERE job_id = ANY(%s)""",
            (job_ids,),
        ).fetchone()
        stop.set()
        await asyncio.gather(*tasks, return_exceptions=True)

        assert succeeded is not None and totals is not None
        print(
            f"\n=== load test: {args.workers} simulated workers, {args.jobs} jobs × "
            f"{args.files} files (N cap {args.files_per_task}) ==="
        )
        print(f"jobs succeeded        {succeeded[0]}/{len(job_ids)} in {elapsed:.1f} s")
        print(
            f"tasks                 {totals[0]} maps + {totals[1]} merges = {stats.done} done "
            f"→ {stats.done / elapsed:.0f} tasks/s (max attempts {totals[2]}, "
            f"abandoned on STOP {stats.stopped})"
        )
        print(
            f"lease latency (ms)    p50 {pct(stats.lease_ms, 50):.1f}  p95 "
            f"{pct(stats.lease_ms, 95):.1f}  p99 {pct(stats.lease_ms, 99):.1f}"
        )
        print(
            f"report latency (ms)   p50 {pct(stats.report_ms, 50):.1f}  p95 "
            f"{pct(stats.report_ms, 95):.1f}  p99 {pct(stats.report_ms, 99):.1f}"
        )
        if stats.spread:
            print(
                f"fairness              in-flight spread across busy jobs: median "
                f"{statistics.median(stats.spread):.0f}, max {max(stats.spread)} "
                f"(over {len(stats.spread)} samples)"
            )
        per_speed = ", ".join(f"{k}: {v}" for k, v in sorted(stats.by_speed.items()))
        print(f"tasks by worker speed {per_speed}  (1x = fastest)")
        if len(ps) == 2:
            print(f"orchestrator process  cpu {ps[0]}%  rss {int(ps[1]) / 1024:.0f} MB")
        return 0 if int(succeeded[0]) == len(job_ids) else 1
    finally:
        stop.set()
        for ch in channels:
            await ch.close()
        orchestrator.terminate()
        orchestrator.wait(timeout=15)
        if job_ids:
            cleanup(conn, job_ids)
        conn.close()


def main() -> None:
    parser = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawTextHelpFormatter
    )
    parser.add_argument("--workers", type=int, default=500)
    parser.add_argument("--jobs", type=int, default=20)
    parser.add_argument("--files", type=int, default=2000)
    parser.add_argument("--files-per-task", type=int, default=10)
    parser.add_argument(
        "--base-secs", type=float, default=0.4, help="work time of a 1x worker per 10 files"
    )
    parser.add_argument("--timeout", type=float, default=300)
    sys.exit(asyncio.run(run(parser.parse_args())))


if __name__ == "__main__":
    main()
