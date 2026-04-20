from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Callable

from config import Settings
from core.contracts.chat import ChatRequest
from core.contracts.tools import ToolSpec
from core.contracts.observability import Tracer
from core.contracts.storage import Storage
from core.tool_runtime.tools.calc import calc
from core.tool_runtime.tools.echo import echo, list_files, read_file, run_command, write_file
from core.tool_runtime.tools.recall import recall
from core.tool_runtime.tools.remember import memory_forget, remember
from core.tool_runtime.tools.time_now import time_now


DEFAULT_ALLOWLIST: set[str] = {"time_now", "remember", "recall", "memory_forget", "calc", "list_files", "read_file", "write_file", "run_command"}
DEFAULT_DENYLIST: set[str] = {"file_write", "http_request", "web_fetch", "run_shell", "exec", "send_message"}


ToolHandler = Callable[[Settings, Storage, Tracer, ChatRequest, dict[str, Any]], dict[str, Any]]


@dataclass(frozen=True)
class ToolRegistry:
    specs: dict[str, ToolSpec]
    handlers: dict[str, ToolHandler]

    def get_spec(self, name: str) -> ToolSpec | None:
        return self.specs.get(name)

    def get_handler(self, name: str) -> ToolHandler | None:
        return self.handlers.get(name)

    def model_tools_spec(self, *, allowlist: set[str]) -> dict[str, Any]:
        out: dict[str, Any] = {}
        for name, spec in self.specs.items():
            if name not in allowlist:
                continue
            if not spec.enabled:
                continue
            out[name] = {
                "desc": spec.desc,
                "risk": spec.risk,
                "side_effect": spec.side_effect,
                "timeout_ms": spec.timeout_ms,
                "json_schema": spec.schema,
            }
        return out


def default_registry() -> ToolRegistry:
    specs: dict[str, ToolSpec] = {
        "time_now": ToolSpec(
            name="time_now",
            desc="获取当前时间（unix+iso, UTC）",
            risk="low",
            side_effect=False,
            timeout_ms=1000,
            schema={"type": "object", "properties": {}, "required": []},
            enabled=True,
        ),
        "remember": ToolSpec(
            name="remember",
            desc="保存一条与当前 conversation 相关的长期笔记（V0 只做文本笔记）",
            risk="low",
            side_effect=False,
            timeout_ms=2000,
            schema={
                "type": "object",
                "properties": {"note": {"type": "string", "minLength": 1, "maxLength": 2000}},
                "required": ["note"],
            },
            enabled=True,
        ),
        "recall": ToolSpec(
            name="recall",
            desc="读取当前 conversation 最近若干条长期笔记",
            risk="low",
            side_effect=False,
            timeout_ms=2000,
            schema={
                "type": "object",
                "properties": {"limit": {"type": "integer", "minimum": 1, "maximum": 50}},
                "required": ["limit"],
            },
            enabled=True,
        ),
        "calc": ToolSpec(
            name="calc",
            desc="做确定性的数学计算，避免模型算错",
            risk="low",
            side_effect=False,
            timeout_ms=2000,
            schema={
                "type": "object",
                "properties": {"expression": {"type": "string", "minLength": 1, "maxLength": 200}},
                "required": ["expression"],
            },
            enabled=True,
        ),
        "memory_forget": ToolSpec(
            name="memory_forget",
            desc="删除当前 conversation 的一条或多条长期笔记",
            risk="low",
            side_effect=False,
            timeout_ms=2000,
            schema={
                "type": "object",
                "properties": {
                    "memory_ids": {
                        "type": "array",
                        "items": {"type": "integer"},
                        "minItems": 1,
                        "maxItems": 50,
                    }
                },
                "required": ["memory_ids"],
            },
            enabled=True,
        ),
        "echo": ToolSpec(
            name="echo",
            desc="调试：原样返回 text",
            risk="low",
            side_effect=False,
            timeout_ms=1000,
            schema={
                "type": "object",
                "properties": {"text": {"type": "string", "minLength": 0, "maxLength": 2000}},
                "required": ["text"],
            },
            enabled=False,
        ),
        "list_files": ToolSpec(
            name="list_files",
            desc="列出项目目录中的文件和子目录（受白名单路径限制）",
            risk="low",
            side_effect=False,
            timeout_ms=3000,
            schema={
                "type": "object",
                "properties": {"path": {"type": "string", "minLength": 1, "maxLength": 2000}, "limit": {"type": "integer", "minimum": 1, "maximum": 500}},
                "required": ["path"],
            },
            enabled=True,
        ),
        "read_file": ToolSpec(
            name="read_file",
            desc="读取项目内文件片段（按行返回）",
            risk="low",
            side_effect=False,
            timeout_ms=3000,
            schema={
                "type": "object",
                "properties": {
                    "file_path": {"type": "string", "minLength": 1, "maxLength": 2000},
                    "offset": {"type": "integer", "minimum": 1, "maximum": 1000000},
                    "limit": {"type": "integer", "minimum": 1, "maximum": 2000},
                },
                "required": ["file_path"],
            },
            enabled=True,
        ),
        "write_file": ToolSpec(
            name="write_file",
            desc="写入项目内文件（原子替换）",
            risk="low",
            side_effect=True,
            timeout_ms=5000,
            schema={
                "type": "object",
                "properties": {
                    "file_path": {"type": "string", "minLength": 1, "maxLength": 2000},
                    "content": {"type": "string", "minLength": 0, "maxLength": 2000000},
                },
                "required": ["file_path", "content"],
            },
            enabled=True,
        ),
        "run_command": ToolSpec(
            name="run_command",
            desc="在项目根目录内执行白名单命令并返回输出",
            risk="low",
            side_effect=True,
            timeout_ms=120000,
            schema={
                "type": "object",
                "properties": {
                    "command": {"type": "string", "minLength": 1, "maxLength": 1000},
                    "timeout_sec": {"type": "integer", "minimum": 1, "maximum": 120},
                },
                "required": ["command"],
            },
            enabled=True,
        ),
    }

    handlers: dict[str, ToolHandler] = {
        "time_now": time_now,
        "remember": remember,
        "recall": recall,
        "memory_forget": memory_forget,
        "calc": calc,
        "echo": echo,
        "list_files": list_files,
        "read_file": read_file,
        "write_file": write_file,
        "run_command": run_command,
    }

    return ToolRegistry(specs=specs, handlers=handlers)


