"""The Dramatiq actor. Kept thin: everything real happens in TaskRunner."""

import dramatiq
from dmean_common.messages import RUN_TASK_ACTOR, TASKS_QUEUE
from dramatiq.middleware import CurrentMessage

from .runner import TaskRunner

MAX_RETRIES = 2  # 3 attempts in total

_runner: TaskRunner | None = None


def set_runner(runner: TaskRunner) -> None:
    global _runner
    _runner = runner


def declare(broker: dramatiq.Broker, time_limit_secs: int) -> dramatiq.Actor[[int], None]:
    """Registers run_task on the broker. Workers consume only `tasks`, never `results`."""
    broker.add_middleware(CurrentMessage())

    def run_task(task_id: int) -> None:
        if _runner is None:
            raise RuntimeError("worker runtime not initialised")
        message = CurrentMessage.get_current_message()
        retries = 0 if message is None else int(message.options.get("retries", 0))
        _runner.run(task_id, last_attempt=retries >= MAX_RETRIES)

    return dramatiq.actor(
        actor_name=RUN_TASK_ACTOR,
        queue_name=TASKS_QUEUE,
        broker=broker,
        max_retries=MAX_RETRIES,
        min_backoff=2_000,
        max_backoff=30_000,
        time_limit=time_limit_secs * 1000,
    )(run_task)
