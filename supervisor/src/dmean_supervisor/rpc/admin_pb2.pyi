from google.protobuf.internal import containers as _containers
from google.protobuf import descriptor as _descriptor
from google.protobuf import message as _message
from collections.abc import Iterable as _Iterable, Mapping as _Mapping
from typing import ClassVar as _ClassVar, Optional as _Optional, Union as _Union

DESCRIPTOR: _descriptor.FileDescriptor

class ListWorkersRequest(_message.Message):
    __slots__ = ()
    def __init__(self) -> None: ...

class WorkerInfo(_message.Message):
    __slots__ = ("worker_id", "busy", "task_id", "job_id", "kind", "attempt", "seconds_on_task", "seconds_since_seen")
    WORKER_ID_FIELD_NUMBER: _ClassVar[int]
    BUSY_FIELD_NUMBER: _ClassVar[int]
    TASK_ID_FIELD_NUMBER: _ClassVar[int]
    JOB_ID_FIELD_NUMBER: _ClassVar[int]
    KIND_FIELD_NUMBER: _ClassVar[int]
    ATTEMPT_FIELD_NUMBER: _ClassVar[int]
    SECONDS_ON_TASK_FIELD_NUMBER: _ClassVar[int]
    SECONDS_SINCE_SEEN_FIELD_NUMBER: _ClassVar[int]
    worker_id: str
    busy: bool
    task_id: int
    job_id: int
    kind: str
    attempt: int
    seconds_on_task: float
    seconds_since_seen: float
    def __init__(self, worker_id: _Optional[str] = ..., busy: _Optional[bool] = ..., task_id: _Optional[int] = ..., job_id: _Optional[int] = ..., kind: _Optional[str] = ..., attempt: _Optional[int] = ..., seconds_on_task: _Optional[float] = ..., seconds_since_seen: _Optional[float] = ...) -> None: ...

class ListWorkersResponse(_message.Message):
    __slots__ = ("workers", "ready_tasks")
    WORKERS_FIELD_NUMBER: _ClassVar[int]
    READY_TASKS_FIELD_NUMBER: _ClassVar[int]
    workers: _containers.RepeatedCompositeFieldContainer[WorkerInfo]
    ready_tasks: int
    def __init__(self, workers: _Optional[_Iterable[_Union[WorkerInfo, _Mapping]]] = ..., ready_tasks: _Optional[int] = ...) -> None: ...
