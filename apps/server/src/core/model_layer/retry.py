from __future__ import annotations

import time
from collections.abc import Callable
from typing import TypeVar


T = TypeVar("T")


def with_retry(*, fn: Callable[[], T], max_retries: int, backoff_s: float) -> T:
    v, _ = with_retry_info(fn=fn, max_retries=max_retries, backoff_s=backoff_s)
    return v


def with_retry_info(
    *,
    fn: Callable[[], T],
    max_retries: int,
    backoff_s: float,
    on_retry: Callable[[int, Exception], None] | None = None,
) -> tuple[T, int]:
    last_err: Exception | None = None
    tries = max(0, int(max_retries)) + 1
    retry_count = 0

    for i in range(tries):
        try:
            return fn(), retry_count
        except Exception as e:
            last_err = e
            if i >= tries - 1:
                break
            retry_count += 1
            if on_retry is not None:
                try:
                    on_retry(retry_count, e)
                except Exception:
                    pass
            sleep_s = max(0.0, float(backoff_s)) * (2**i)
            if sleep_s > 0:
                time.sleep(sleep_s)

    if last_err is not None:
        raise last_err
    raise RuntimeError("retry_failed")

