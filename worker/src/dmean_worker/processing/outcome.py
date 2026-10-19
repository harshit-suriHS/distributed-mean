"""What running a task produced. Independent of gRPC: the client turns it into a Report."""

from dataclasses import dataclass
from enum import StrEnum


class FailureType(StrEnum):
    INTERNAL = "internal"
    USER_INPUT = "user_input"


@dataclass(frozen=True)
class Done:
    output_key: str
    size_bytes: int


@dataclass(frozen=True)
class Failed:
    failure_type: FailureType
    retryable: bool  # the orchestrator retries with backoff; otherwise it fails the job
    error: str


@dataclass(frozen=True)
class Abandoned:
    """The orchestrator said STOP (job failed, lease lost): nothing to report."""


Outcome = Done | Failed | Abandoned
