from __future__ import annotations

import re
from typing import Any


_SENSITIVE_KEY_RE = re.compile(r"(key|token|secret|password|authorization|cookie|session)", re.IGNORECASE)


def redact(obj: Any) -> Any:
    if isinstance(obj, dict):
        out: dict[str, Any] = {}
        for k, v in obj.items():
            ks = str(k)
            if _SENSITIVE_KEY_RE.search(ks):
                out[ks] = "***"
            else:
                out[ks] = redact(v)
        return out

    if isinstance(obj, list):
        return [redact(x) for x in obj]

    return obj

