from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path


def _load_env_file(path: Path) -> None:
    if not path.exists() or not path.is_file():
        return

    for line in path.read_text(encoding="utf-8").splitlines():
        s = line.strip()
        if not s or s.startswith("#"):
            continue
        if "=" not in s:
            continue

        k, v = s.split("=", 1)
        key = k.strip()
        val = v.strip().strip('"').strip("'")
        if key and key not in os.environ:
            os.environ[key] = val


@dataclass(frozen=True)
class Settings:
    sqlite_path: str
    siliconflow_base_url: str
    siliconflow_api_key: str | None
    siliconflow_model: str
    project_root: str
    health_p95_threshold_ms: int = 1000
    health_penalty_per_100ms: int = 1
    health_penalty_cap: int = 40
    health_green_min_score: int = 90
    health_yellow_min_score: int = 70
    health_yellow_max_failure_rate: float = 20.0
    health_empty_score: int = 50
    summary_id_salt: str = ""
    agent_exec_enabled: bool = True
    group_mode_enabled: bool = True


def _env_int(name: str, default: int) -> int:
    try:
        return int(os.environ.get(name, str(default)))
    except Exception:
        return int(default)


def _env_float(name: str, default: float) -> float:
    try:
        return float(os.environ.get(name, str(default)))
    except Exception:
        return float(default)


def _env_bool(name: str, default: bool) -> bool:
    v = str(os.environ.get(name, "1" if default else "0")).strip().lower()
    if v in {"1", "true", "yes", "on"}:
        return True
    if v in {"0", "false", "no", "off"}:
        return False
    return bool(default)


def load_settings() -> Settings:
    server_dir = Path(__file__).resolve().parents[1]
    _load_env_file(server_dir / ".env.local")

    sqlite_path = os.environ.get("AGUNT_SQLITE_PATH", str(server_dir / "agunt_v0.sqlite3"))
    base_url = os.environ.get("SILICONFLOW_BASE_URL", "https://api.siliconflow.cn/v1")
    api_key = os.environ.get("SILICONFLOW_API_KEY")
    model = os.environ.get("SILICONFLOW_MODEL", "Qwen/Qwen2.5-7B-Instruct")
    project_root = os.environ.get("AGUNT_PROJECT_ROOT", str(server_dir.parents[1]))

    return Settings(
        sqlite_path=sqlite_path,
        siliconflow_base_url=base_url,
        siliconflow_api_key=api_key,
        siliconflow_model=model,
        project_root=project_root,
        health_p95_threshold_ms=_env_int("AGUNT_HEALTH_P95_THRESHOLD_MS", 1000),
        health_penalty_per_100ms=_env_int("AGUNT_HEALTH_PENALTY_PER_100MS", 1),
        health_penalty_cap=_env_int("AGUNT_HEALTH_PENALTY_CAP", 40),
        health_green_min_score=_env_int("AGUNT_HEALTH_GREEN_MIN_SCORE", 90),
        health_yellow_min_score=_env_int("AGUNT_HEALTH_YELLOW_MIN_SCORE", 70),
        health_yellow_max_failure_rate=_env_float("AGUNT_HEALTH_YELLOW_MAX_FAILURE_RATE", 20.0),
        health_empty_score=_env_int("AGUNT_HEALTH_EMPTY_SCORE", 50),
        summary_id_salt=os.environ.get("AGUNT_SUMMARY_ID_SALT", ""),
        agent_exec_enabled=_env_bool("AGUNT_AGENT_EXEC_ENABLED", True),
        group_mode_enabled=_env_bool("AGUNT_GROUP_MODE_ENABLED", True),
    )
