from __future__ import annotations

import os
import time
from collections import deque

from fastapi import HTTPException, Request


_WINDOW_S = 60.0
_BUCKETS: dict[str, deque[float]] = {}


def enforce_rate_limit(request: Request) -> None:
    rpm_s = os.environ.get("AGUNT_WEB_RPM")
    if not rpm_s:
        return

    try:
        rpm = int(rpm_s)
    except Exception:
        return

    if rpm <= 0:
        return

    ip = getattr(request.client, "host", None) or "unknown"

    now = time.time()
    q = _BUCKETS.get(ip)
    if q is None:
        q = deque()
        _BUCKETS[ip] = q

    while q and (now - q[0]) > _WINDOW_S:
        q.popleft()

    if len(q) >= rpm:
        raise HTTPException(status_code=429, detail="rate_limited")

    q.append(now)

