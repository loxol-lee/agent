from __future__ import annotations

import os

from fastapi import HTTPException, Request


def require_api_key(request: Request) -> None:
    expected = os.environ.get("AGUNT_WEB_API_KEY")
    if not expected:
        return

    got = request.headers.get("x-agunt-key")
    if got and got == expected:
        return

    auth = request.headers.get("authorization") or request.headers.get("Authorization")
    if auth:
        s = auth.strip()
        if s.lower().startswith("bearer "):
            token = s[7:].strip()
            if token == expected:
                return

    raise HTTPException(status_code=401, detail="unauthorized")

