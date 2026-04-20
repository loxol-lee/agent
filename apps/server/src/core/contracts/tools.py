from __future__ import annotations

from dataclasses import dataclass
from typing import Any


ERROR_TOOL_NOT_FOUND = "TOOL_NOT_FOUND"
ERROR_TOOL_NOT_ALLOWED = "TOOL_NOT_ALLOWED"
ERROR_TOOL_ARGS_INVALID = "TOOL_ARGS_INVALID"
ERROR_TOOL_TIMEOUT = "TOOL_TIMEOUT"
ERROR_TOOL_RUNTIME_ERROR = "TOOL_RUNTIME_ERROR"


def tool_error_user_message(code: str | None) -> str:
    if code == ERROR_TOOL_NOT_ALLOWED:
        return "当前会话不允许使用该工具"
    if code == ERROR_TOOL_ARGS_INVALID:
        return "工具参数不符合要求，请重试"
    if code == ERROR_TOOL_TIMEOUT:
        return "工具执行超时，请稍后重试"
    if code == ERROR_TOOL_RUNTIME_ERROR:
        return "工具执行失败，请稍后重试"
    if code == ERROR_TOOL_NOT_FOUND:
        return "工具不可用，请稍后重试"
    return "工具执行失败，请稍后重试"


@dataclass(frozen=True)
class ToolSpec:
    name: str
    desc: str
    risk: str
    side_effect: bool
    timeout_ms: int
    schema: dict[str, Any]
    enabled: bool = True


@dataclass(frozen=True)
class ToolCall:
    call_id: str
    name: str
    args: dict[str, Any]


@dataclass(frozen=True)
class ToolError:
    code: str
    message: str | None = None


@dataclass(frozen=True)
class ToolResult:
    call_id: str
    name: str
    ok: bool
    content: dict[str, Any]
    error: ToolError | None = None

