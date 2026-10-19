"""A task as the worker sees it: what to read, where to write, whether to run at all."""

from dataclasses import dataclass

from dmean_worker.storage.keys import partial_key, result_key

FINISHED_TASK_STATUSES = frozenset({"done", "failed", "skipped"})


@dataclass(frozen=True)
class Task:
    id: int
    job_id: int
    job_uuid: str
    kind: str  # map | merge
    no_of_files: int
    is_final: bool
    status: str
    no_of_columns: int
    job_status: str
    input_keys: list[str]

    @property
    def output_key(self) -> str:
        return result_key(self.job_uuid) if self.is_final else partial_key(self.job_uuid, self.id)

    @property
    def should_run(self) -> bool:
        return self.job_status == "running" and self.status not in FINISHED_TASK_STATUSES
