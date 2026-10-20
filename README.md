# Distributed Mean

## The problem

Compute the **mean of every index** across `F` CSV files, each holding `C` numbers, using a fleet of `W` workers.

| Symbol | Meaning | Range |
|---|---|---|
| `F` | number of files | `1 < F < 100k` |
| `C` | numbers in each file | `1 < C < 1M` |
| `W` | number of workers (system-wide) | configurable |

For example, with `F = 2` and `C = 3`:

```
file 1:  1, 2, 3
file 2:  4, 5, 6
result:  2.5, 3.5, 4.5
```

Constraints:
- A worker can hold **at most 5 files in memory at a time**, so a large job must be split up.
- **Workers run at different speeds**, so no worker should sit idle or wait on a slower one.
- **Several jobs can run at once**, submitted while others are still going.

The user uploads the files and creates a job through an API; the workers' outputs are combined into one result file.

## Start it up

Requires [Docker](https://docs.docker.com/get-docker/) and [uv](https://docs.astral.sh/uv/).

```sh
cp .env.example .env
docker compose up -d --build
```

This starts Postgres, MinIO (S3-compatible storage), the API, the orchestrator, the supervisor (which then starts the worker containers) and the UI.

- **Dashboard:** http://localhost:8080. Submit a job, set W, watch the workers and download results.
- **API:** http://localhost:3000

Or try it from the command line:

```sh
uv run scripts/demo.py --values "1,2,3" "4,5,6"      # the example above → [2.5, 3.5, 4.5]
uv run scripts/demo.py --files 50 --columns 1000     # 50 random files of 1,000 numbers
```

Stop everything (worker containers included):

```sh
docker compose down --remove-orphans
```
