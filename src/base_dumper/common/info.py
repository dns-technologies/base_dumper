from typing import NamedTuple


INFO_TEMPLATE = """Execution query on host {info.host}
┌─────────────────────────────────────────┐
│            Query information            │
╞═══════════════╤═════════════════════════╡
│ Query Kind    │ {info.kind:>23} │
├───────────────┼─────────────────────────┤
│ Duration      │ {info.duration:>15g} seconds │
├───────────────┼─────────────────────────┤
│ Memory Usage  │ {info.memory:>17,} bytes │
├───────────────┼─────────────────────────┤
│ Storage Usage │ {info.storage:>17,} bytes │
├───────────────┼─────────────────────────┤
│ Total Count   │ {info.rows:>18,} rows │
└───────────────┴─────────────────────────┘
"""


class DebugInfo(NamedTuple):
    """Debug information to log."""

    host: str
    kind: str
    duration: float
    memory: int = 0
    storage: int = 0
    rows: int = 0

    def __repr__(self) -> str:
        """String representation of DebugInfo."""

        return INFO_TEMPLATE.format(info=self)
