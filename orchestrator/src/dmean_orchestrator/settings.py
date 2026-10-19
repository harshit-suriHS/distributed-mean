"""Orchestrator settings, read from env (and the repo's .env when present)."""

from pathlib import Path

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

    postgres_host: str = "localhost"
    postgres_port: int = 5432
    postgres_db: str
    orchestrator_db_password: str
    db_pool_size: int = 10

    s3_endpoint: str
    s3_region: str = "us-east-1"
    s3_access_key: str
    s3_secret_key: str
    s3_bucket: str

    advisory_lock_key: int = 4242  # only one orchestrator holds it; a second copy waits
    cleanup_interval_secs: float = 300.0  # sweep: interrupted cleanups, expired uploads

    grpc_host: str = "0.0.0.0"
    grpc_port: int = 50051

    # Sizing (design §7)
    target_task_secs: float = 25.0
    min_task_secs: float = 5.0
    default_rate: float = 5_000_000.0  # values/s one worker parses (measured: ~0.2 s per 1M)
    max_files_per_task: int = 5000

    # Leases (design §9)
    lease_secs: float = 30.0
    heartbeat_secs: int = 10
    max_lease_secs: float = 250.0  # ~10 × target task duration: catches hung workers
    max_attempts: int = 4
    min_backoff_secs: float = 2.0
    max_backoff_secs: float = 30.0
    max_wait_secs: int = 20  # longest Lease long-poll
    worker_alive_secs: float = 30.0  # seen within this window = live
    timer_tick_secs: float = 0.25  # upper bound between timer checks

    log_level: str = "info"

    @property
    def db_dsn(self) -> str:
        return (
            f"host={self.postgres_host} port={self.postgres_port} dbname={self.postgres_db} "
            f"user=orchestrator password={self.orchestrator_db_password}"
        )
