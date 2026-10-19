"""A leased task, as the orchestrator sends it: everything needed to run it, no database."""

from dataclasses import dataclass

from dmean_worker.rpc import task_queue_pb2 as pb


@dataclass(frozen=True)
class Task:
    task_id: int
    attempt: int  # fencing token: sent back with every heartbeat and report
    kind: str  # map | merge
    input_keys: list[str]
    no_of_files: int
    no_of_columns: int
    is_final: bool
    output_key: str
    heartbeat_seconds: int = 10

    @classmethod
    def from_proto(cls, task: pb.Task) -> "Task":
        return cls(
            task_id=task.task_id,
            attempt=task.attempt,
            kind=task.kind,
            input_keys=list(task.input_keys),
            no_of_files=task.no_of_files,
            no_of_columns=task.no_of_columns,
            is_final=task.is_final,
            output_key=task.output_key,
            heartbeat_seconds=task.heartbeat_seconds or 10,
        )
