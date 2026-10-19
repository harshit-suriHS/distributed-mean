"""gRPC Admin service: read-only views of the orchestrator's in-memory state, for the API/UI."""

import asyncio
from typing import Any

from dmean_orchestrator.rpc import admin_pb2 as pb
from dmean_orchestrator.rpc import admin_pb2_grpc as pb_grpc
from dmean_orchestrator.rpc.service import TaskQueueService
from dmean_orchestrator.scheduling.fair_queue import TaskState


class AdminService(pb_grpc.AdminServicer):
    def __init__(self, queue_service: TaskQueueService) -> None:
        self._svc = queue_service

    async def ListWorkers(
        self, request: pb.ListWorkersRequest, context: Any
    ) -> pb.ListWorkersResponse:
        now = asyncio.get_running_loop().time()
        queue = self._svc.queue
        busy = {
            t.lease.worker_id: t
            for t in queue.tasks.values()
            if t.state is TaskState.LEASED and t.lease is not None
        }
        workers: list[pb.WorkerInfo] = []
        for worker_id in self._svc.live_worker_ids():
            info = pb.WorkerInfo(
                worker_id=worker_id,
                seconds_since_seen=now - self._svc.workers[worker_id],
            )
            task = busy.get(worker_id)
            if task is not None and task.lease is not None:
                info.busy = True
                info.task_id = task.task_id
                info.job_id = task.job_id
                info.kind = task.kind.value
                info.attempt = task.attempt
                info.seconds_on_task = now - task.lease.started
            workers.append(info)
        workers.sort(key=lambda w: w.worker_id)
        ready = sum(1 for t in queue.tasks.values() if t.state is TaskState.READY)
        return pb.ListWorkersResponse(workers=workers, ready_tasks=ready)
