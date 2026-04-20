from __future__ import annotations

import re
from dataclasses import dataclass
from typing import Any

from core.contracts.tools import (
    ERROR_TOOL_ARGS_INVALID,
    ERROR_TOOL_NOT_ALLOWED,
    ERROR_TOOL_NOT_FOUND,
    ToolSpec,
)


_TOOL_NAME_RE = re.compile(r"^[a-zA-Z0-9_]+$")
_CALC_EXPR_RE = re.compile(r"^[0-9\s\+\-\*\/\(\)\.]+$")


@dataclass(frozen=True)
class PolicyDecision:
    allowed: bool
    error_code: str | None = None
    reason: str | None = None


class ToolPolicy:
    def __init__(
        self,
        *,
        allowlist: set[str],
        denylist: set[str],
        allow_network: bool = False,
    ):
        self.allowlist = set(allowlist)
        self.denylist = set(denylist)
        self.allow_network = bool(allow_network)

    def decide(self, *, spec: ToolSpec | None, name: str, args: Any) -> PolicyDecision:
        if not _TOOL_NAME_RE.fullmatch(name or ""):
            return PolicyDecision(False, ERROR_TOOL_NOT_ALLOWED, "invalid_tool_name")
        if name in self.denylist:
            return PolicyDecision(False, ERROR_TOOL_NOT_ALLOWED, "denylist")
        if name not in self.allowlist:
            return PolicyDecision(False, ERROR_TOOL_NOT_ALLOWED, "not_in_allowlist")
        if spec is None:
            return PolicyDecision(False, ERROR_TOOL_NOT_FOUND, "unknown")

        if not spec.enabled:
            return PolicyDecision(False, ERROR_TOOL_NOT_ALLOWED, "disabled")
        if str(spec.risk).lower() != "low":
            return PolicyDecision(False, ERROR_TOOL_NOT_ALLOWED, "risk")
        if bool(spec.side_effect) and name not in {"write_file", "run_command"}:
            return PolicyDecision(False, ERROR_TOOL_NOT_ALLOWED, "side_effect")

        ok, err = _validate_json_schema(spec.schema, args)
        if not ok:
            return PolicyDecision(False, err or ERROR_TOOL_ARGS_INVALID, "schema")

        if name == "calc":
            expr = (args or {}).get("expression")
            if not isinstance(expr, str):
                return PolicyDecision(False, ERROR_TOOL_ARGS_INVALID, "calc_expression_type")
            if not _CALC_EXPR_RE.fullmatch(expr.strip()):
                return PolicyDecision(False, ERROR_TOOL_ARGS_INVALID, "calc_expression_chars")

        return PolicyDecision(True, None, None)


def _validate_json_schema(schema: dict[str, Any], args: Any) -> tuple[bool, str | None]:
    if not isinstance(schema, dict):
        return False, ERROR_TOOL_ARGS_INVALID

    t = schema.get("type")
    if t != "object":
        return False, ERROR_TOOL_ARGS_INVALID

    if not isinstance(args, dict):
        return False, ERROR_TOOL_ARGS_INVALID

    required = schema.get("required")
    if required is not None:
        if not isinstance(required, list):
            return False, ERROR_TOOL_ARGS_INVALID
        for k in required:
            if k not in args:
                return False, ERROR_TOOL_ARGS_INVALID

    props = schema.get("properties")
    if props is None:
        return True, None
    if not isinstance(props, dict):
        return False, ERROR_TOOL_ARGS_INVALID

    for k, rule in props.items():
        if k not in args:
            continue
        if not isinstance(rule, dict):
            return False, ERROR_TOOL_ARGS_INVALID

        v = args.get(k)
        rt = rule.get("type")

        if rt == "string":
            if not isinstance(v, str):
                return False, ERROR_TOOL_ARGS_INVALID
            min_len = rule.get("minLength")
            max_len = rule.get("maxLength")
            if isinstance(min_len, int) and len(v) < min_len:
                return False, ERROR_TOOL_ARGS_INVALID
            if isinstance(max_len, int) and len(v) > max_len:
                return False, ERROR_TOOL_ARGS_INVALID

        elif rt == "integer":
            if not isinstance(v, int):
                return False, ERROR_TOOL_ARGS_INVALID
            mn = rule.get("minimum")
            mx = rule.get("maximum")
            if isinstance(mn, int) and v < mn:
                return False, ERROR_TOOL_ARGS_INVALID
            if isinstance(mx, int) and v > mx:
                return False, ERROR_TOOL_ARGS_INVALID

        elif rt == "array":
            if not isinstance(v, list):
                return False, ERROR_TOOL_ARGS_INVALID
            min_items = rule.get("minItems")
            max_items = rule.get("maxItems")
            if isinstance(min_items, int) and len(v) < min_items:
                return False, ERROR_TOOL_ARGS_INVALID
            if isinstance(max_items, int) and len(v) > max_items:
                return False, ERROR_TOOL_ARGS_INVALID
            item_rule = rule.get("items")
            if isinstance(item_rule, dict):
                item_type = item_rule.get("type")
                if item_type == "integer":
                    for item in v:
                        if not isinstance(item, int):
                            return False, ERROR_TOOL_ARGS_INVALID

        elif rt is None:
            continue
        else:
            return False, ERROR_TOOL_ARGS_INVALID

    return True, None

