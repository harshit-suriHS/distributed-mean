"""
The contract between orchestrator and workers: queue names, actor names and payloads.

Each side sends raw Dramatiq messages by name, so neither has to import (and so register,
and so consume) the other's actors.
"""

from enum import StrEnum

import dramatiq
from pydantic import BaseModel

TASKS_QUEUE = "tasks"
RESULTS_QUEUE = "results"
RUN_TASK_ACTOR = "run_task"
REPORT_ACTOR = "report"


class ReportEvent(StrEnum):
    STARTED = "started"
    DONE = "done"
    FAILED = "failed"


class FailureType(StrEnum):
    INTERNAL = "internal"
    USER_INPUT = "user_input"


class Report(BaseModel):
    """Worker → orchestrator. The orchestrator is the only one that writes it to Postgres."""

    task_id: int
    event: ReportEvent
    worker_id: str
    # done: where the output was written and its size, for the `files` row
    output_key: str | None = None
    size_bytes: int | None = None
    # failed
    failure_type: FailureType | None = None
    retryable: bool = True
    error: str | None = None


def run_task_message(task_id: int) -> dramatiq.Message[None]:
    return dramatiq.Message(
        queue_name=TASKS_QUEUE, actor_name=RUN_TASK_ACTOR, args=(task_id,), kwargs={}, options={}
    )


def report_message(report: Report) -> dramatiq.Message[None]:
    return dramatiq.Message(
        queue_name=RESULTS_QUEUE,
        actor_name=REPORT_ACTOR,
        args=(),
        kwargs={"report": report.model_dump(mode="json")},
        options={},
    )
