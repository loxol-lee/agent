from __future__ import annotations

from enum import Enum


class StopReason(str, Enum):
    completed = "completed"
    max_steps = "max_steps"
    tool_call_limit = "tool_call_limit"
    time_budget = "time_budget"
    max_tokens = "max_tokens"
    model_error = "model_error"
    tool_error = "tool_error"
    user_cancelled = "user_cancelled"

