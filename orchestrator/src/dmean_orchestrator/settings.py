"""Orchestrator settings, read from env (and the repo's .env when present)."""

from pathlib import Path

from pydantic_settings import BaseSettings, SettingsConfigDict

from dmean_orchestrator.scheduling.planner import Sizing


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

    advisory_lock_key: int = 4242  # only one orchestrator holds it; a second copy waits
    cleanup_after_hours: float = (
        24.0  # dmean-cleanup deletes a job's inputs/partials this long after it ends
    )

    grpc_host: str = "0.0.0.0"
    grpc_port: int = 50051

    # Sizing: every task is ~T seconds of work, from C alone (never from the number of workers)
    target_task_secs: float = 10.0
    request_secs: float = 0.008  # per-GET latency, measured ~8 ms under load locally
    bytes_per_value: float = 19.3  # CSV bytes per value (random floats, measured)
    bandwidth_mb_per_sec: float = 150.0  # one GET stream from MinIO, measured locally
    parse_values_per_sec: float = 5_000_000.0  # np.loadtxt on one core, measured (~0.2 s per 1M)
    max_files_per_task: int = 5000

    @property
    def sizing(self) -> "Sizing":
        return Sizing(
            target_task_secs=self.target_task_secs,
            request_secs=self.request_secs,
            bytes_per_value=self.bytes_per_value,
            bandwidth_bytes_per_sec=self.bandwidth_mb_per_sec * 1e6,
            parse_values_per_sec=self.parse_values_per_sec,
            max_files_per_task=self.max_files_per_task,
        )

    # Leases (design §9)
    lease_secs: float = 30.0
    heartbeat_secs: int = 10
    max_lease_secs: float = 100.0  # 10 × target task length: catches hung workers
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


class CleanupSettings(Settings):
    """dmean-cleanup also talks to storage; the orchestrator itself never does."""

    s3_endpoint: str
    s3_region: str = "us-east-1"
    s3_access_key: str
    s3_secret_key: str
    s3_bucket: str
