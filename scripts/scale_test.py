# /// script
# requires-python = ">=3.13"
# dependencies = ["numpy==2.5.4", "requests==2.34.2"]
# ///
"""
Scale test through the public API, as users would: upload J jobs of F files × C values each,
then submit all J jobs at once and watch them run.

To avoid formatting billions of floats, a pool of distinct random CSV files is generated once
and reused across uploads; every job's expected mean is still known exactly, so every result
is checked.

    uv run scripts/scale_test.py --jobs 20 --files 20000 --columns 2000 --workers 5

Needs the stack running (`docker compose up`).
"""

import argparse
import statistics
import sys
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass, field

import numpy as np
import requests

local = threading.local()


def session() -> requests.Session:
    if not hasattr(local, "s"):
        local.s = requests.Session()
    return local.s


@dataclass
class Job:
    index: int
    upload: dict
    picks: np.ndarray  # pool index used for each file
    expected: np.ndarray
    id: str = ""
    submitted: float = 0.0
    finished: float = 0.0
    status: str = "uploading"
    samples: list[float] = field(default_factory=list)  # percent over time


def fmt(seconds: float) -> str:
    return f"{seconds / 60:.1f} min" if seconds >= 90 else f"{seconds:.1f} s"


def main() -> int:
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawTextHelpFormatter)
    p.add_argument("--api", default="http://localhost:3000")
    p.add_argument("--jobs", type=int, default=20)
    p.add_argument("--files", type=int, default=20_000)
    p.add_argument("--columns", type=int, default=2_000)
    p.add_argument("--pool", type=int, default=200, help="distinct random files to reuse")
    p.add_argument("--workers", type=int, default=5, help="set W before starting")
    p.add_argument("--parallel", type=int, default=64, help="concurrent uploads")
    a = p.parse_args()
    api = a.api

    # ---- W ---------------------------------------------------------------------------------
    requests.put(f"{api}/workers", json={"desired_workers": a.workers}, timeout=10).raise_for_status()
    deadline = time.monotonic() + 90
    while time.monotonic() < deadline:
        live = requests.get(f"{api}/workers", timeout=10).json()["live"]
        if live == a.workers:
            break
        time.sleep(2)
    print(f"W = {a.workers}, live workers: {live}")

    # ---- the pool of distinct files ----------------------------------------------------------
    rng = np.random.default_rng(42)
    pool = rng.random((a.pool, a.columns))
    pool_csv = [(",".join(map(repr, row.tolist())) + "\n").encode() for row in pool]
    file_bytes = len(pool_csv[0])
    total_bytes = file_bytes * a.files * a.jobs
    print(f"{a.jobs} jobs × {a.files:,} files × {a.columns:,} values; "
          f"~{file_bytes / 1024:.0f} KB per file, {total_bytes / 1e9:.1f} GB to upload")

    jobs: list[Job] = []
    for j in range(a.jobs):
        picks = (np.arange(a.files) * 31 + j * 7919) % a.pool
        counts = np.bincount(picks, minlength=a.pool)
        expected = (counts @ pool) / a.files
        upload = session().post(f"{api}/uploads", timeout=10).json()
        jobs.append(Job(j, upload, picks, expected))

    # ---- upload everything -------------------------------------------------------------------
    def put(job: Job, i: int) -> None:
        up = job.upload
        fields = {**up["fields"], "key": f"{up['prefix']}file_{i:06d}.csv"}
        for attempt in range(5):
            try:
                r = session().post(up["url"], data=fields,
                                   files={"file": pool_csv[job.picks[i]]}, timeout=120)
                if r.status_code == 204:
                    return
            except requests.RequestException:
                pass
            time.sleep(0.5 * (attempt + 1))
        raise RuntimeError(f"upload failed: job {job.index} file {i}")

    started = time.monotonic()
    total = a.files * a.jobs
    done = 0
    next_report = 0.1
    with ThreadPoolExecutor(a.parallel) as ex:
        futures = [ex.submit(put, job, i) for job in jobs for i in range(a.files)]
        for f in as_completed(futures):
            f.result()
            done += 1
            if done / total >= next_report:
                el = time.monotonic() - started
                print(f"  uploaded {done:,}/{total:,} ({done / total:.0%}) in {fmt(el)} "
                      f"— {done / el:,.0f} files/s, {done * file_bytes / el / 1e6:,.0f} MB/s")
                next_report += 0.1
    upload_secs = time.monotonic() - started
    print(f"upload done in {fmt(upload_secs)}")

    # ---- submit all jobs at once -------------------------------------------------------------
    def submit(job: Job) -> None:
        r = session().post(f"{api}/jobs", timeout=300, json={
            "upload_id": job.upload["upload_id"], "no_of_files": a.files,
            "no_of_columns": a.columns})
        r.raise_for_status()
        job.id = r.json()["id"]
        job.submitted = time.monotonic()
        job.status = "validating"

    started = time.monotonic()
    with ThreadPoolExecutor(a.jobs) as ex:
        list(ex.map(submit, jobs))
    print(f"submitted {a.jobs} jobs in {fmt(time.monotonic() - started)} (each is then validated "
          f"in the background: one listing of {a.files:,} files)")

    # ---- watch -------------------------------------------------------------------------------
    by_id = {job.id: job for job in jobs}
    busy_samples: list[float] = []
    started = time.monotonic()
    last_print = 0.0
    while True:
        listed = session().get(f"{api}/jobs?limit=200", timeout=10).json()["jobs"]
        now = time.monotonic()
        for item in listed:
            job = by_id.get(item["id"])
            if job is None:
                continue
            job.samples.append(item["percent"])
            if item["status"] in ("succeeded", "failed") and not job.finished:
                job.finished = now
            job.status = item["status"]
        try:
            w = session().get(f"{api}/workers", timeout=10).json()
            if w["live"]:
                busy_samples.append(w["busy"] / w["live"])
        except requests.RequestException:
            pass
        finished = sum(1 for j in jobs if j.finished)
        if now - last_print >= 5:
            running = [j.samples[-1] for j in jobs if not j.finished and j.samples]
            spread = (max(running) - min(running)) if len(running) > 1 else 0
            validating = sum(1 for j in jobs if j.status == "validating")
            print(f"  {fmt(now - started):>8}: {finished}/{a.jobs} done, {validating} validating, "
                  f"busy {busy_samples[-1] if busy_samples else 0:.0%}, "
                  f"progress spread across running jobs {spread}%")
            last_print = now
        if finished == a.jobs:
            break
        time.sleep(1)
    run_secs = time.monotonic() - started

    # ---- results -----------------------------------------------------------------------------
    def check(job: Job) -> bool:
        if job.status != "succeeded":
            return False
        url = session().get(f"{api}/jobs/{job.id}/result", timeout=10).json()["url"]
        means = np.array([float(v) for v in session().get(url, timeout=60).text.split(",")])
        return bool(np.allclose(means, job.expected, rtol=1e-12))

    with ThreadPoolExecutor(8) as ex:
        correct = list(ex.map(check, jobs))
    durations = [j.finished - j.submitted for j in jobs]
    values = a.jobs * a.files * a.columns
    print(f"\n=== {a.jobs} jobs × {a.files:,} files × {a.columns:,} values, W = {a.workers} ===")
    print(f"upload                {fmt(upload_secs)} ({total_bytes / 1e9:.1f} GB, "
          f"{total / upload_secs:,.0f} files/s)")
    print(f"processing (all jobs) {fmt(run_secs)} → {values / run_secs / 1e6:,.1f}M values/s, "
          f"{total / run_secs:,.0f} files/s")
    print(f"per job               min {fmt(min(durations))}, median "
          f"{fmt(statistics.median(durations))}, max {fmt(max(durations))}")
    if busy_samples:
        print(f"workers busy          {statistics.mean(busy_samples):.0%} of the time on average")
    print(f"results               {sum(correct)}/{a.jobs} succeeded and equal the expected mean")
    return 0 if all(correct) else 1


if __name__ == "__main__":
    sys.exit(main())
