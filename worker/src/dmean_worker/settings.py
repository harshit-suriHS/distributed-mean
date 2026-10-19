"""Worker settings, read from env (and the repo's .env when present)."""

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
    worker_db_password: str

    redis_host: str = "localhost"
    redis_port: int = 6379
    redis_db: int = 0

    s3_endpoint: str
    s3_region: str = "us-east-1"
    s3_access_key: str
    s3_secret_key: str
    s3_bucket: str

    task_time_limit_secs: int = 250  # ~10 × target task duration: catches hangs
    job_check_secs: float = 3.0  # how often a running task re-reads its job's status
    heartbeat_ttl_secs: int = 30
    heartbeat_every_secs: int = 10

    log_level: str = "info"

    @property
    def redis_url(self) -> str:
        return f"redis://{self.redis_host}:{self.redis_port}/{self.redis_db}"

    def postgres_dsn(self, user: str, password: str) -> str:
        return (
            f"host={self.postgres_host} port={self.postgres_port} dbname={self.postgres_db} "
            f"user={user} password={password}"
        )

    @property
    def db_dsn(self) -> str:
        return self.postgres_dsn("worker_ro", self.worker_db_password)
