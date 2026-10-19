"""Supervisor settings (env and the repo's .env). Worker processes it starts inherit the env."""

import sys
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


class SupervisorSettings(BaseSettings):
    model_config = SettingsConfigDict(env_file=find_env_file(), extra="ignore")

    postgres_host: str = "localhost"
    postgres_port: int = 5432
    postgres_db: str
    supervisor_db_password: str

    orchestrator_addr: str = "localhost:50051"
    worker_id_prefix: str = "worker"  # children are named {prefix}-{n}
    # the worker package is installed in this service's environment
    worker_command: list[str] = [sys.executable, "-m", "dmean_worker.main"]

    reconcile_secs: float = 5.0  # also woken at once by NOTIFY settings_changed
    startup_grace_secs: float = 30.0  # a new worker isn't judged before this
    hang_after_secs: float = 60.0  # alive but unseen by the orchestrator this long → SIGKILL
    drain_timeout_secs: float = 300.0  # on shutdown: wait this long for workers to finish

    log_level: str = "info"

    @property
    def db_dsn(self) -> str:
        return (
            f"host={self.postgres_host} port={self.postgres_port} dbname={self.postgres_db} "
            f"user=supervisor password={self.supervisor_db_password}"
        )
