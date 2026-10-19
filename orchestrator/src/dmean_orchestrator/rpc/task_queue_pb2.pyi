from google.protobuf.internal import containers as _containers
from google.protobuf.internal import enum_type_wrapper as _enum_type_wrapper
from google.protobuf import descriptor as _descriptor
from google.protobuf import message as _message
from collections.abc import Iterable as _Iterable, Mapping as _Mapping
from typing import ClassVar as _ClassVar, Optional as _Optional, Union as _Union

DESCRIPTOR: _descriptor.FileDescriptor

class FailureType(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
    __slots__ = ()
    INTERNAL: _ClassVar[FailureType]
    USER_INPUT: _ClassVar[FailureType]
INTERNAL: FailureType
USER_INPUT: FailureType

class LeaseRequest(_message.Message):
    __slots__ = ("worker_id", "wait_seconds")
    WORKER_ID_FIELD_NUMBER: _ClassVar[int]
    WAIT_SECONDS_FIELD_NUMBER: _ClassVar[int]
    worker_id: str
    wait_seconds: int
    def __init__(self, worker_id: _Optional[str] = ..., wait_seconds: _Optional[int] = ...) -> None: ...

class LeaseResponse(_message.Message):
    __slots__ = ("task",)
    TASK_FIELD_NUMBER: _ClassVar[int]
    task: Task
    def __init__(self, task: _Optional[_Union[Task, _Mapping]] = ...) -> None: ...

class Task(_message.Message):
    __slots__ = ("task_id", "attempt", "kind", "input_keys", "no_of_files", "no_of_columns", "is_final", "output_key", "heartbeat_seconds")
    TASK_ID_FIELD_NUMBER: _ClassVar[int]
    ATTEMPT_FIELD_NUMBER: _ClassVar[int]
    KIND_FIELD_NUMBER: _ClassVar[int]
    INPUT_KEYS_FIELD_NUMBER: _ClassVar[int]
    NO_OF_FILES_FIELD_NUMBER: _ClassVar[int]
    NO_OF_COLUMNS_FIELD_NUMBER: _ClassVar[int]
    IS_FINAL_FIELD_NUMBER: _ClassVar[int]
    OUTPUT_KEY_FIELD_NUMBER: _ClassVar[int]
    HEARTBEAT_SECONDS_FIELD_NUMBER: _ClassVar[int]
    task_id: int
    attempt: int
    kind: str
    input_keys: _containers.RepeatedScalarFieldContainer[str]
    no_of_files: int
    no_of_columns: int
    is_final: bool
    output_key: str
    heartbeat_seconds: int
    def __init__(self, task_id: _Optional[int] = ..., attempt: _Optional[int] = ..., kind: _Optional[str] = ..., input_keys: _Optional[_Iterable[str]] = ..., no_of_files: _Optional[int] = ..., no_of_columns: _Optional[int] = ..., is_final: _Optional[bool] = ..., output_key: _Optional[str] = ..., heartbeat_seconds: _Optional[int] = ...) -> None: ...

class HeartbeatRequest(_message.Message):
    __slots__ = ("worker_id", "task_id", "attempt")
    WORKER_ID_FIELD_NUMBER: _ClassVar[int]
    TASK_ID_FIELD_NUMBER: _ClassVar[int]
    ATTEMPT_FIELD_NUMBER: _ClassVar[int]
    worker_id: str
    task_id: int
    attempt: int
    def __init__(self, worker_id: _Optional[str] = ..., task_id: _Optional[int] = ..., attempt: _Optional[int] = ...) -> None: ...

class HeartbeatResponse(_message.Message):
    __slots__ = ("action",)
    class Action(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
        __slots__ = ()
        CONTINUE: _ClassVar[HeartbeatResponse.Action]
        STOP: _ClassVar[HeartbeatResponse.Action]
    CONTINUE: HeartbeatResponse.Action
    STOP: HeartbeatResponse.Action
    ACTION_FIELD_NUMBER: _ClassVar[int]
    action: HeartbeatResponse.Action
    def __init__(self, action: _Optional[_Union[HeartbeatResponse.Action, str]] = ...) -> None: ...

class ReportRequest(_message.Message):
    __slots__ = ("worker_id", "task_id", "attempt", "done", "failed")
    WORKER_ID_FIELD_NUMBER: _ClassVar[int]
    TASK_ID_FIELD_NUMBER: _ClassVar[int]
    ATTEMPT_FIELD_NUMBER: _ClassVar[int]
    DONE_FIELD_NUMBER: _ClassVar[int]
    FAILED_FIELD_NUMBER: _ClassVar[int]
    worker_id: str
    task_id: int
    attempt: int
    done: Done
    failed: Failed
    def __init__(self, worker_id: _Optional[str] = ..., task_id: _Optional[int] = ..., attempt: _Optional[int] = ..., done: _Optional[_Union[Done, _Mapping]] = ..., failed: _Optional[_Union[Failed, _Mapping]] = ...) -> None: ...

class Done(_message.Message):
    __slots__ = ("output_key", "size_bytes")
    OUTPUT_KEY_FIELD_NUMBER: _ClassVar[int]
    SIZE_BYTES_FIELD_NUMBER: _ClassVar[int]
    output_key: str
    size_bytes: int
    def __init__(self, output_key: _Optional[str] = ..., size_bytes: _Optional[int] = ...) -> None: ...

class Failed(_message.Message):
    __slots__ = ("failure_type", "retryable", "error")
    FAILURE_TYPE_FIELD_NUMBER: _ClassVar[int]
    RETRYABLE_FIELD_NUMBER: _ClassVar[int]
    ERROR_FIELD_NUMBER: _ClassVar[int]
    failure_type: FailureType
    retryable: bool
    error: str
    def __init__(self, failure_type: _Optional[_Union[FailureType, str]] = ..., retryable: _Optional[bool] = ..., error: _Optional[str] = ...) -> None: ...

class ReportResponse(_message.Message):
    __slots__ = ()
    def __init__(self) -> None: ...
