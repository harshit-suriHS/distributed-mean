"""Settings shared by every Python service, read from env (and the repo's .env when present)."""

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


class CommonSettings(BaseSettings):
    model_config = SettingsConfigDict(env_file=find_env_file(), extra="ignore")

    postgres_host: str = "localhost"
    postgres_port: int = 5432
    postgres_db: str

    redis_host: str = "localhost"
    redis_port: int = 6379
    redis_db: int = 0

    s3_endpoint: str
    s3_region: str = "us-east-1"
    s3_access_key: str
    s3_secret_key: str
    s3_bucket: str

    log_level: str = "info"

    @property
    def redis_url(self) -> str:
        return f"redis://{self.redis_host}:{self.redis_port}/{self.redis_db}"

    def postgres_dsn(self, user: str, password: str) -> str:
        return (
            f"host={self.postgres_host} port={self.postgres_port} dbname={self.postgres_db} "
            f"user={user} password={password}"
        )
