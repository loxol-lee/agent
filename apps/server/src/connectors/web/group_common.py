from __future__ import annotations

from fastapi import APIRouter


def build_group_router(*, tag: str) -> APIRouter:
    return APIRouter(tags=[tag])


__all__ = ["build_group_router"]
