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

    grpc_host: str = "0.0.0.0"
    grpc_port: int = 50051

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
