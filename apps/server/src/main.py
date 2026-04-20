from __future__ import annotations

import os
from pathlib import Path

from fastapi import FastAPI

from config import load_settings
from connectors.web.routes import router as web_router
from core.infra.observability.tracer import Tracer
from core.infra.storage.sqlite_store import SQLiteStore
from core.tool_runtime.executor import ToolExecutor
from core.tool_runtime.policy import ToolPolicy
from core.tool_runtime.registry import DEFAULT_ALLOWLIST, DEFAULT_DENYLIST, default_registry


def create_app() -> FastAPI:
    app = FastAPI()

    settings = load_settings()
    src_dir = Path(__file__).resolve().parent
    schema_path = src_dir / "core" / "infra" / "storage" / "schema.sql"

    store = SQLiteStore(settings.sqlite_path, schema_sql_path=str(schema_path))
    tracer = Tracer(store)

    registry = default_registry()
    policy = ToolPolicy(allowlist=DEFAULT_ALLOWLIST, denylist=DEFAULT_DENYLIST)
    executor = ToolExecutor(registry=registry, policy=policy)
    tools_spec = registry.model_tools_spec(allowlist=DEFAULT_ALLOWLIST)

    app.state.settings = settings
    app.state.store = store
    app.state.tracer = tracer
    app.state.tool_executor = executor
    app.state.tools_spec = tools_spec

    app.include_router(web_router)
    return app


app = create_app()


def run() -> None:
    try:
        import uvicorn
    except Exception as e:
        raise RuntimeError("uvicorn is not installed. Run: python3 -m pip install -r requirements.txt") from e

    host = os.environ.get("AGUNT_HOST", "127.0.0.1")
    port = int(os.environ.get("AGUNT_PORT", "8000"))
    reload_env = os.environ.get("AGUNT_RELOAD", "1").lower()
    reload = reload_env not in {"0", "false", "no"}

    src_dir = Path(__file__).resolve().parent
    uvicorn.run(
        "main:app",
        host=host,
        port=port,
        reload=reload,
        reload_dirs=[str(src_dir)],
    )


if __name__ == "__main__":
    run()
