"""Stands in for dmean-worker in tests: runs until SIGTERM, then exits cleanly (a drain)."""

import signal
import sys
import time


def drain(signum: int, frame: object) -> None:
    sys.exit(0)


signal.signal(signal.SIGTERM, drain)
while True:
    time.sleep(0.05)
