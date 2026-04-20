from __future__ import annotations

import os

from config import Settings


def route_models(settings: Settings, alias: str | None = None) -> list[str]:
    alias_key = (alias or "").strip()

    if alias_key:
        env_key = "AGUNT_MODEL_ALIAS_" + alias_key.upper()
        mapped = os.environ.get(env_key) or ""
        items = [x.strip() for x in mapped.split(",") if x.strip()]
        if items:
            out: list[str] = []
            for m in items:
                if m and m not in out:
                    out.append(m)
            return out

    primary = settings.siliconflow_model

    fb = os.environ.get("SILICONFLOW_FALLBACK_MODEL") or ""
    fallbacks = [x.strip() for x in fb.split(",") if x.strip()]

    out: list[str] = []
    if primary:
        out.append(primary)
    for m in fallbacks:
        if m and m not in out:
            out.append(m)
    return out

