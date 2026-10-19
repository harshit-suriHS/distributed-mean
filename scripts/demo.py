# /// script
# requires-python = ">=3.13"
# dependencies = ["numpy==2.5.4", "requests==2.34.2"]
# ///
"""
Try the system end to end, as a user: generate F random CSV files of C values, upload them
through the API's presigned policy, submit the job, wait, and check the mean against numpy.

    uv run scripts/demo.py --files 50 --columns 1000
    uv run scripts/demo.py --files 2 --columns 3 --values "1,2,3" "4,5,6"   # the brief's example

Needs the stack running (`docker compose up`).
"""

import argparse
import sys
import time
from concurrent.futures import ThreadPoolExecutor

import numpy as np
import requests


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawTextHelpFormatter)
    parser.add_argument("--api", default="http://localhost:3000")
    parser.add_argument("--files", type=int, default=20)
    parser.add_argument("--columns", type=int, default=100)
    parser.add_argument("--values", nargs="*", help="explicit rows, e.g. 1,2,3 4,5,6")
    parser.add_argument("--parallel", type=int, default=16, help="concurrent uploads")
    args = parser.parse_args()

    if args.values:
        rows = np.array([[float(v) for v in row.split(",")] for row in args.values])
    else:
        rows = np.random.default_rng().random((args.files, args.columns))
    files, columns = rows.shape

    upload = requests.post(f"{args.api}/uploads", timeout=10).json()
    print(f"upload {upload['upload_id']}: sending {files} files of {columns} values …")

    def send(i: int) -> None:
        body = ",".join(map(repr, rows[i].tolist())) + "\n"
        fields = {**upload["fields"], "key": f"{upload['prefix']}file_{i:06d}.csv"}
        resp = requests.post(upload["url"], data=fields, files={"file": body}, timeout=60)
        resp.raise_for_status()

    started = time.monotonic()
    with ThreadPoolExecutor(args.parallel) as pool:
        list(pool.map(send, range(files)))
    print(f"uploaded in {time.monotonic() - started:.1f} s")

    resp = requests.post(
        f"{args.api}/jobs",
        json={"upload_id": upload["upload_id"], "no_of_files": files, "no_of_columns": columns},
        timeout=120,
    )
    if resp.status_code != 202:
        print(f"submit failed: {resp.status_code} {resp.text}")
        return 1
    job_id = resp.json()["id"]
    print(f"job {job_id} submitted")

    started = time.monotonic()
    last = ""
    while True:
        job = requests.get(f"{args.api}/jobs/{job_id}", timeout=10).json()
        line = (f"  {job['status']:<9} {job['progress']['percent']:>3}% of files summed, "
                f"tasks {job['tasks']}")
        if line != last:
            print(line)
            last = line
        if job["status"] in ("succeeded", "failed"):
            break
        time.sleep(0.5)
    print(f"finished in {time.monotonic() - started:.1f} s")
    if job["status"] == "failed":
        print(f"failed ({job['failure_type']}): {job['error']}")
        return 1

    result = requests.get(f"{args.api}/jobs/{job_id}/result", timeout=10)
    if result.status_code != 200:
        print(f"result download not available yet ({result.status_code})")
        return 0
    means = np.array([float(v) for v in requests.get(result.json()["url"], timeout=60).text.split(",")])
    ok = np.allclose(means, rows.mean(axis=0), rtol=1e-12)
    preview = ", ".join(f"{v:.6g}" for v in means[:5]) + (" …" if columns > 5 else "")
    print(f"result: [{preview}]")
    print(f"matches numpy mean: {ok}")
    return 0 if ok else 1


if __name__ == "__main__":
    sys.exit(main())
