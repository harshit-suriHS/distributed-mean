"""worker:<id> key with a TTL: the orchestrator counts live workers, the supervisor spots hangs."""

import contextlib
import json
import os
import socket
import threading
import time
from typing import Any

import dramatiq
import redis
import structlog

log = structlog.get_logger()


def make_worker_id() -> str:
    return f"{socket.gethostname()}-{os.getpid()}"


class Heartbeat(dramatiq.Middleware):
    def __init__(self, client: redis.Redis, worker_id: str, ttl_secs: int, every_secs: int) -> None:
        self._client = client
        self._key = f"worker:{worker_id}"
        self._value = json.dumps(
            {"pid": os.getpid(), "host": socket.gethostname(), "since": time.time()}
        )
        self._ttl = ttl_secs
        self._every = every_secs
        self._stop = threading.Event()
        self._thread: threading.Thread | None = None

    def after_worker_boot(self, broker: dramatiq.Broker, worker: Any) -> None:
        self._thread = threading.Thread(target=self._loop, name="heartbeat", daemon=True)
        self._thread.start()

    def before_worker_shutdown(self, broker: dramatiq.Broker, worker: Any) -> None:
        self._stop.set()
        if self._thread is not None:
            self._thread.join(timeout=5)
        # leave the live count right away, don't wait for the TTL
        with contextlib.suppress(redis.RedisError):
            self._client.delete(self._key)

    def _loop(self) -> None:
        while not self._stop.is_set():
            try:
                self._client.set(self._key, self._value, ex=self._ttl)
            except redis.RedisError as err:
                log.warning("heartbeat failed", error=str(err))
            self._stop.wait(self._every)
