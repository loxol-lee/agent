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
    )
