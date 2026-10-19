"""
Integration fixtures: real MinIO, and a FAKE orchestrator (an in-process gRPC TaskQueue server
with scripted tasks) so the worker's side of the protocol is tested on its own.
"""

import threading
import uuid
from collections import deque
from collections.abc import Iterator
from concurrent import futures
from dataclasses import dataclass, field
from typing import Any

import grpc
import pytest

from dmean_worker.rpc import task_queue_pb2 as pb
from dmean_worker.rpc import task_queue_pb2_grpc as pb_grpc
from dmean_worker.settings import Settings
from dmean_worker.storage.s3 import Storage


@pytest.fixture(scope="session")
def settings() -> Settings:
    return Settings(lease_wait_secs=1, report_give_up_secs=10)  # pyright: ignore[reportCallIssue]


@pytest.fixture(scope="session")
def storage(settings: Settings) -> Storage:
    return Storage(settings)


@dataclass
class FakeTaskQueue(pb_grpc.TaskQueueServicer):
    """Hands out scripted tasks, records heartbeats and reports. `stop_tasks` answer STOP."""

    tasks: deque[pb.Task] = field(default_factory=deque[pb.Task])
    heartbeats: list[int] = field(default_factory=list[int])
    reports: list[pb.ReportRequest] = field(default_factory=list[pb.ReportRequest])
    stop_tasks: set[int] = field(default_factory=set[int])
    reported: threading.Event = field(default_factory=threading.Event)
    fail_reports: bool = False  # simulate a crashing orchestrator: Report → UNAVAILABLE
    report_attempts: int = 0

    def Lease(self, request: pb.LeaseRequest, context: Any) -> pb.LeaseResponse:
        if self.tasks:
            return pb.LeaseResponse(task=self.tasks.popleft())
        context.add_callback(lambda: None)
        threading.Event().wait(min(request.wait_seconds, 0.2))
        return pb.LeaseResponse()

    def Heartbeat(self, request: pb.HeartbeatRequest, context: Any) -> pb.HeartbeatResponse:
        self.heartbeats.append(request.task_id)
        if request.task_id in self.stop_tasks:
            return pb.HeartbeatResponse(action=pb.HeartbeatResponse.STOP)
        return pb.HeartbeatResponse(action=pb.HeartbeatResponse.CONTINUE)

    def Report(self, request: pb.ReportRequest, context: Any) -> pb.ReportResponse:
        self.report_attempts += 1
        if self.fail_reports:
            context.abort(grpc.StatusCode.UNAVAILABLE, "orchestrator restarting")
        self.reports.append(request)
        self.reported.set()
        return pb.ReportResponse()


@dataclass
class FakeOrchestrator:
    queue: FakeTaskQueue
    address: str


def start_fake(queue: FakeTaskQueue, port: int = 0) -> tuple[grpc.Server, int]:
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=8))
    pb_grpc.add_TaskQueueServicer_to_server(queue, server)
    bound = server.add_insecure_port(f"127.0.0.1:{port}")
    server.start()
    return server, bound


@pytest.fixture
def fake() -> Iterator[FakeOrchestrator]:
    queue = FakeTaskQueue()
    server, port = start_fake(queue)
    yield FakeOrchestrator(queue, f"127.0.0.1:{port}")
    server.stop(grace=None)


@dataclass
class Objects:
    """Puts test objects in MinIO under a unique prefix and removes them afterwards."""

    storage: Storage
    prefix: str = field(default_factory=lambda: f"test-worker/{uuid.uuid4()}/")

    def put(self, name: str, text: str) -> str:
        key = self.prefix + name
        self.storage.put_text(key, text)
        return key

    def key(self, name: str) -> str:
        return self.prefix + name

    def cleanup(self) -> None:
        s3 = self.storage._s3  # pyright: ignore[reportPrivateUsage]
        listed = s3.list_objects_v2(Bucket=self.storage.bucket, Prefix=self.prefix)
        keys = [{"Key": o["Key"]} for o in listed.get("Contents", [])]
        if keys:
            s3.delete_objects(Bucket=self.storage.bucket, Delete={"Objects": keys})


@pytest.fixture
def objects(storage: Storage) -> Iterator[Objects]:
    o = Objects(storage)
    yield o
    o.cleanup()


def make_task(
    task_id: int,
    keys: list[str],
    output_key: str,
    *,
    kind: str = "map",
    columns: int = 3,
    final: bool = False,
    no_of_files: int | None = None,
    heartbeat_seconds: int = 1,
) -> pb.Task:
    return pb.Task(
        task_id=task_id,
        attempt=1,
        kind=kind,
        input_keys=keys,
        no_of_files=len(keys) if no_of_files is None else no_of_files,
        no_of_columns=columns,
        is_final=final,
        output_key=output_key,
        heartbeat_seconds=heartbeat_seconds,
    )
