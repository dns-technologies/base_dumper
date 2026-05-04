from enum import (
    Enum,
    IntEnum,
)
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
└───────────────┴─────────────────────────┘"""


class DumpFormat(Enum):
    """Enum for dump type format."""

    BINARY = 0
    CSV = 1


class IsolationLevel(str, Enum):
    """Transaction isolation level."""

    UNCOMMITTED = "READ UNCOMMITTED"
    COMMITTED = "READ COMMITTED"
    REPEATABLE = "REPEATABLE READ"
    SERIALIZABLE = "SERIALIZABLE"


class DumperMode(IntEnum):
    """Mode level for dumper."""

    TEST = 0
    DEBUG = 1
    PROD = 2


class DBConnector(NamedTuple):
    """Base connector."""

    host: str
    dbname: str
    user: str
    password: str
    port: int


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


class DBMetadata(NamedTuple):
    """Database object."""

    name: str
    version: str
    columns: dict

    def to_bytes(self) -> None: ...
    def to_rows(self) -> None: ...
    def to_pandas(self) -> None: ...
    def to_polars(self) -> None: ...
    def close(self) -> None: ...
    def tell(self) -> int: return 0
