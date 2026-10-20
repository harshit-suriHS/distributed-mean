"""Worker settings, read from env (and the repo's .env when present)."""

from pathlib import Path

from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


def find_env_file(start: Path | None = None) -> Path | None:
    """Nearest .env walking up from start (the repo root .env in local dev)."""
    here = (start or Path.cwd()).resolve()
    for folder in (here, *here.parents):
        candidate = folder / ".env"
        if candidate.is_file():
            return candidate
    return None


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=find_env_file(), extra="ignore")

    worker_id: str | None = None  # default: hostname-pid (on Kubernetes: the pod name)
    orchestrator_addr: str = "localhost:50051"
    lease_wait_secs: int = 20  # long-poll: how long one Lease call waits for work
    report_give_up_secs: float = 300.0  # keep retrying a report this long while unreachable

    s3_endpoint: str
    s3_region: str = "us-east-1"
    s3_access_key: str
    s3_secret_key: str
    s3_bucket: str

    # Files fetched ahead while one is summed: ≤ 4, so with the accumulator ≤ 5 files in memory
    prefetch_files: int = Field(default=4, ge=1, le=4)

    # Local demos of heterogeneous speed: extra seconds per input file (0 in production)
    slowdown_secs_per_file: float = 0.0

    log_level: str = "info"
