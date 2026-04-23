from __future__ import annotations

from pathlib import Path


BASE_DIR = Path("/Users/lixiaoyi/Documents/newtext/agunt")


DIRS = [
    "apps/web-chat/src",
    "apps/web-chat/public",
    "apps/server/src/connectors/web",
    "apps/server/src/core/contracts",
    "apps/server/src/core/orchestrator",
    "apps/server/src/core/model_layer/providers",
    "apps/server/src/core/tool_runtime/tools",
    "apps/server/src/core/infra/storage",
    "apps/server/src/core/infra/observability/writers",
]

FILES = [
    "apps/web-chat/README.md",
    "apps/web-chat/src/index.html",
    "apps/web-chat/src/app.tsx",
    "apps/web-chat/src/api.ts",
    "apps/web-chat/src/types.ts",
    "apps/server/src/main.py",
    "apps/server/src/config.py",
    "apps/server/src/connectors/web/routes.py",
    "apps/server/src/connectors/web/auth.py",
    "apps/server/src/connectors/web/rate_limit.py",
    "apps/server/src/core/contracts/chat.py",
    "apps/server/src/core/contracts/model.py",
    "apps/server/src/core/contracts/tools.py",
    "apps/server/src/core/contracts/observability.py",
    "apps/server/src/core/contracts/storage.py",
    "apps/server/src/core/orchestrator/agent_runtime.py",
    "apps/server/src/core/orchestrator/context_builder.py",
    "apps/server/src/core/orchestrator/budgets.py",
    "apps/server/src/core/orchestrator/stop_reason.py",
    "apps/server/src/core/model_layer/router.py",
    "apps/server/src/core/model_layer/client.py",
    "apps/server/src/core/model_layer/retry.py",
    "apps/server/src/core/model_layer/limits.py",
    "apps/server/src/core/model_layer/providers/siliconflow.py",
    "apps/server/src/core/model_layer/providers/local_stub.py",
    "apps/server/src/core/tool_runtime/registry.py",
    "apps/server/src/core/tool_runtime/executor.py",
    "apps/server/src/core/tool_runtime/policy.py",
    "apps/server/src/core/tool_runtime/redact.py",
    "apps/server/src/core/tool_runtime/tools/time_now.py",
    "apps/server/src/core/tool_runtime/tools/remember.py",
    "apps/server/src/core/tool_runtime/tools/recall.py",
    "apps/server/src/core/tool_runtime/tools/calc.py",
    "apps/server/src/core/tool_runtime/tools/echo.py",
    "apps/server/src/core/infra/storage/sqlite_store.py",
    "apps/server/src/core/infra/storage/schema.sql",
    "apps/server/src/core/infra/observability/tracer.py",
    "apps/server/src/core/infra/observability/writers/sqlite_writer.py",
    "apps/server/src/core/infra/observability/writers/jsonl_writer.py",
]


def main() -> None:
    if not BASE_DIR.exists():
        raise SystemExit(f"Base dir does not exist: {BASE_DIR}")

    created_dirs = 0
    for d in DIRS:
        p = BASE_DIR / d
        if not p.exists():
            p.mkdir(parents=True, exist_ok=True)
            created_dirs += 1
        else:
            p.mkdir(parents=True, exist_ok=True)

    created_files = 0
    for f in FILES:
        p = BASE_DIR / f
        p.parent.mkdir(parents=True, exist_ok=True)
        if not p.exists():
            p.touch(exist_ok=True)
            created_files += 1

    print(f"Base: {BASE_DIR}")
    print(f"Dirs ensured: {len(DIRS)} (new: {created_dirs})")
    print(f"Files ensured: {len(FILES)} (new: {created_files})")
    print("Done.")


if __name__ == "__main__":
    main()