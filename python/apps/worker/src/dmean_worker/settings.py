from dmean_common.config import CommonSettings


class WorkerSettings(CommonSettings):
    worker_db_password: str

    task_time_limit_secs: int = 250  # ~10 × target task duration: catches hangs
    job_check_secs: float = 3.0  # how often a running task re-reads its job's status
    heartbeat_ttl_secs: int = 30
    heartbeat_every_secs: int = 10

    @property
    def db_dsn(self) -> str:
        return self.postgres_dsn("worker_ro", self.worker_db_password)
