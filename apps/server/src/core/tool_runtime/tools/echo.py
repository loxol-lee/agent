from __future__ import annotations

import shlex
import subprocess
from pathlib import Path
from tempfile import NamedTemporaryFile
from typing import Any

from config import Settings
from core.contracts.chat import ChatRequest
from core.contracts.observability import Tracer
from core.contracts.storage import Storage


def _safe_path(settings: Settings, raw_path: str) -> Path:
    root = Path(str(settings.project_root)).resolve()
    target = (root / raw_path).resolve() if not Path(raw_path).is_absolute() else Path(raw_path).resolve()
    if root != target and root not in target.parents:
        raise ValueError("path_not_allowed")
    return target


def echo(settings: Settings, store: Storage, tracer: Tracer, req: ChatRequest, args: dict[str, Any]) -> dict[str, Any]:
    return {"text": str(args.get("text") or "")}


def list_files(settings: Settings, store: Storage, tracer: Tracer, req: ChatRequest, args: dict[str, Any]) -> dict[str, Any]:
    path = str(args.get("path") or ".")
    limit = int(args.get("limit") or 200)
    limit = max(1, min(limit, 500))
    p = _safe_path(settings, path)
    if not p.exists():
        raise ValueError("path_not_found")
    if not p.is_dir():
        raise ValueError("path_not_dir")

    items: list[dict[str, Any]] = []
    for i, child in enumerate(sorted(p.iterdir(), key=lambda x: x.name.lower())):
        if i >= limit:
            break
        items.append(
            {
                "name": child.name,
                "path": str(child),
                "is_dir": child.is_dir(),
                "size": (child.stat().st_size if child.is_file() else None),
            }
        )
    return {"root": str(Path(str(settings.project_root)).resolve()), "path": str(p), "items": items, "truncated": len(items) >= limit}


def read_file(settings: Settings, store: Storage, tracer: Tracer, req: ChatRequest, args: dict[str, Any]) -> dict[str, Any]:
    file_path = str(args.get("file_path") or "").strip()
    if not file_path:
        raise ValueError("file_path_required")
    offset = int(args.get("offset") or 1)
    limit = int(args.get("limit") or 400)
    offset = max(1, offset)
    limit = max(1, min(limit, 2000))

    p = _safe_path(settings, file_path)
    if not p.exists() or not p.is_file():
        raise ValueError("file_not_found")

    lines = p.read_text(encoding="utf-8").splitlines()
    start = offset - 1
    end = min(len(lines), start + limit)
    out = [{"line": idx + 1, "text": lines[idx]} for idx in range(start, end)]
    return {"file_path": str(p), "offset": offset, "limit": limit, "total_lines": len(lines), "lines": out}


def write_file(settings: Settings, store: Storage, tracer: Tracer, req: ChatRequest, args: dict[str, Any]) -> dict[str, Any]:
    file_path = str(args.get("file_path") or "").strip()
    content = args.get("content")
    if not file_path:
        raise ValueError("file_path_required")
    if not isinstance(content, str):
        raise ValueError("content_required")

    p = _safe_path(settings, file_path)
    p.parent.mkdir(parents=True, exist_ok=True)
    with NamedTemporaryFile("w", encoding="utf-8", delete=False, dir=str(p.parent)) as tmp:
        tmp.write(content)
        tmp_path = Path(tmp.name)
    tmp_path.replace(p)
    return {"ok": True, "file_path": str(p), "bytes": len(content.encode("utf-8"))}


def run_command(settings: Settings, store: Storage, tracer: Tracer, req: ChatRequest, args: dict[str, Any]) -> dict[str, Any]:
    command = str(args.get("command") or "").strip()
    if not command:
        raise ValueError("command_required")
    timeout_sec = int(args.get("timeout_sec") or 30)
    timeout_sec = max(1, min(timeout_sec, 120))

    parts = shlex.split(command)
    if not parts:
        raise ValueError("command_required")

    allowed_bins = {"python", "python3", "pip", "git"}
    if parts[0] not in allowed_bins:
        raise ValueError("command_not_allowed")

    cwd = str(Path(str(settings.project_root)).resolve())
    proc = subprocess.run(parts, cwd=cwd, capture_output=True, text=True, timeout=timeout_sec)
    stdout = (proc.stdout or "")[:8000]
    stderr = (proc.stderr or "")[:8000]
    return {
        "ok": proc.returncode == 0,
        "command": command,
        "cwd": cwd,
        "exit_code": int(proc.returncode),
        "stdout": stdout,
        "stderr": stderr,
    }
