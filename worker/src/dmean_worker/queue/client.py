"""
gRPC client for the orchestrator's TaskQueue: Lease (long-poll), Heartbeat, Report.

The orchestrator owns retries, attempts and backoff; the worker only has to keep asking and
make sure every report eventually arrives (Report is idempotent, so retrying is safe).
"""

import threading
import time

import grpc
import structlog

from dmean_worker.processing.outcome import Done, Failed, FailureType
from dmean_worker.processing.task import Task
from dmean_worker.rpc import task_queue_pb2 as pb
from dmean_worker.rpc import task_queue_pb2_grpc as pb_grpc

log = structlog.get_logger()

FAILURE_TYPES = {FailureType.INTERNAL: pb.INTERNAL, FailureType.USER_INPUT: pb.USER_INPUT}


class TaskQueueClient:
    def __init__(self, address: str, worker_id: str) -> None:
        self.worker_id = worker_id
        self._channel = grpc.insecure_channel(address)
        self._stub = pb_grpc.TaskQueueStub(self._channel)

    def lease(self, wait_secs: int, cancel: threading.Event) -> Task | None:
        """Waits up to wait_secs for a task. Returns early (None) if `cancel` is set, so a
        draining worker doesn't sit in a long-poll. Raises grpc.RpcError if unreachable."""
        call = self._stub.Lease.future(
            pb.LeaseRequest(worker_id=self.worker_id, wait_seconds=wait_secs),
            timeout=wait_secs + 5,
        )
        while not call.done():
            if cancel.wait(0.1):
                call.cancel()
                return None
        response = call.result()
        return Task.from_proto(response.task) if response.HasField("task") else None

    def heartbeat(self, task: Task) -> bool:
        """True to keep going, False if the orchestrator said STOP. If it's unreachable we
        keep working: either it comes back, or the lease expires and someone else retries."""
        try:
            response = self._stub.Heartbeat(
                pb.HeartbeatRequest(
                    worker_id=self.worker_id, task_id=task.task_id, attempt=task.attempt
                ),
                timeout=5,
            )
        except grpc.RpcError as err:
            log.warning("heartbeat failed", task_id=task.task_id, error=str(err.code()))
            return True
        return response.action != pb.HeartbeatResponse.STOP

    def report(self, task: Task, outcome: Done | Failed, give_up_secs: float) -> None:
        """Retries until the orchestrator answers. Gives up after give_up_secs: by then the
        lease has long expired and the task was retried elsewhere."""
        request = pb.ReportRequest(
            worker_id=self.worker_id, task_id=task.task_id, attempt=task.attempt
        )
        if isinstance(outcome, Done):
            request.done.CopyFrom(
                pb.Done(output_key=outcome.output_key, size_bytes=outcome.size_bytes)
            )
        else:
            request.failed.CopyFrom(
                pb.Failed(
                    failure_type=FAILURE_TYPES[outcome.failure_type],
                    retryable=outcome.retryable,
                    error=outcome.error,
                )
            )
        delay, deadline = 1.0, time.monotonic() + give_up_secs
        while True:
            try:
                self._stub.Report(request, timeout=10)
                return
            except grpc.RpcError as err:
                if time.monotonic() > deadline:
                    log.error("giving up on a report", task_id=task.task_id, code=str(err.code()))
                    return
                log.warning("orchestrator unreachable, holding the report", retry_in=delay)
                time.sleep(delay)
                delay = min(delay * 2, 30.0)

    def close(self) -> None:
        self._channel.close()
