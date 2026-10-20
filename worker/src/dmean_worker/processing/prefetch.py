"""
Ordered prefetch: fetch the next few inputs while the current one is parsed.

S3 has no batch GET, so parallel fetching means several GETs in flight. With a window of W,
at most W files are held at once (being fetched, waiting, or being summed); with the
accumulator that's W + 1, so W ≤ 4 keeps the brief's "≤ 5 files at a time".

Files are handed over in key order even though they download in parallel, so the sum is
bit-for-bit the same as reading them one by one (a retried task writes identical bytes).
"""

from collections import deque
from collections.abc import Callable, Iterable, Iterator
from concurrent.futures import Future, ThreadPoolExecutor

MAX_WINDOW = 4  # + the accumulator = 5 files in memory


def prefetched(
    keys: Iterable[str], load: Callable[[str], str], window: int
) -> Iterator[tuple[str, str]]:
    """Yields (key, text) in order while up to `window` loads run ahead."""
    window = max(1, min(window, MAX_WINDOW))
    if window == 1:
        for key in keys:
            yield key, load(key)
        return

    pool = ThreadPoolExecutor(max_workers=window, thread_name_prefix="prefetch")
    pending: deque[tuple[str, Future[str]]] = deque()
    remaining = iter(keys)

    def fill() -> None:
        while len(pending) < window:
            key = next(remaining, None)
            if key is None:
                return
            pending.append((key, pool.submit(load, key)))

    try:
        fill()
        while pending:
            key, future = pending.popleft()
            text = future.result()  # a failed GET raises here: the task fails and is retried
            yield key, text
            fill()  # only after the caller is done with this file: the window stays ≤ W
    finally:
        # stopped early or failed: drop queued GETs (running ones finish in the background)
        pool.shutdown(wait=False, cancel_futures=True)
