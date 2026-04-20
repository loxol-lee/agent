from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class Budgets:
    history_limit: int = 20
    memory_limit: int = 8
    max_steps: int = 4
    tool_call_limit: int = 6
    max_input_chars: int = 20000
    time_budget_ms: int = 60_000

    @property
    def max_tool_steps(self) -> int:
        return self.tool_call_limit

