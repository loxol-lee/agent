from __future__ import annotations

import ast
from typing import Any

from config import Settings
from core.contracts.chat import ChatRequest
from core.contracts.observability import Tracer
from core.contracts.storage import Storage


def calc(settings: Settings, store: Storage, tracer: Tracer, req: ChatRequest, args: dict[str, Any]) -> dict[str, Any]:
    expr = str(args.get("expression") or "")
    return {"value": safe_calc(expr)}


def safe_calc(expr: str) -> str:
    node = ast.parse(expr, mode="eval")

    allowed = (
        ast.Expression,
        ast.BinOp,
        ast.UnaryOp,
        ast.Add,
        ast.Sub,
        ast.Mult,
        ast.Div,
        ast.Pow,
        ast.USub,
        ast.UAdd,
        ast.Constant,
    )

    for n in ast.walk(node):
        if not isinstance(n, allowed):
            raise ValueError("unsupported expression")
        if isinstance(n, ast.Constant) and not isinstance(n.value, (int, float)):
            raise ValueError("unsupported constant")

    val = eval(compile(node, "<calc>", "eval"), {"__builtins__": {}}, {})
    if isinstance(val, bool):
        raise ValueError("unsupported value")
    if not isinstance(val, (int, float)):
        raise ValueError("unsupported value")
    return str(val)
