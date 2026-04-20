from __future__ import annotations

import os
from dataclasses import dataclass


@dataclass(frozen=True)
class ModelLimits:
    max_retries: int = 1
    backoff_s: float = 0.5
    max_concurrency: int = 8
    rpm: int = 0
    tpm: int = 0
    chars_per_token: float = 4.0


def load_model_limits() -> ModelLimits:
    mr = os.environ.get("AGUNT_MODEL_MAX_RETRIES")
    bo = os.environ.get("AGUNT_MODEL_BACKOFF_S")
    mc = os.environ.get("AGUNT_MODEL_MAX_CONCURRENCY")
    rpm_s = os.environ.get("AGUNT_MODEL_RPM")
    tpm_s = os.environ.get("AGUNT_MODEL_TPM")
    cpt_s = os.environ.get("AGUNT_MODEL_CHARS_PER_TOKEN")

    max_retries = 1
    backoff_s = 0.5
    max_concurrency = 8
    rpm = 0
    tpm = 0
    chars_per_token = 4.0

    try:
        if mr is not None:
            max_retries = max(0, int(mr))
    except Exception:
        max_retries = 1

    try:
        if bo is not None:
            backoff_s = float(bo)
    except Exception:
        backoff_s = 0.5

    try:
        if mc is not None:
            max_concurrency = max(1, int(mc))
    except Exception:
        max_concurrency = 8

    try:
        if rpm_s is not None:
            rpm = max(0, int(rpm_s))
    except Exception:
        rpm = 0

    try:
        if tpm_s is not None:
            tpm = max(0, int(tpm_s))
    except Exception:
        tpm = 0

    try:
        if cpt_s is not None:
            chars_per_token = max(0.5, float(cpt_s))
    except Exception:
        chars_per_token = 4.0

    return ModelLimits(
        max_retries=max_retries,
        backoff_s=backoff_s,
        max_concurrency=max_concurrency,
        rpm=rpm,
        tpm=tpm,
        chars_per_token=chars_per_token,
    )
