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
│ Memory Usage  │ {info.memory_string:>23} │
├───────────────┼─────────────────────────┤
│ Storage Usage │ {info.storage_string:>23} │
├───────────────┼─────────────────────────┤
│ Total Count   │ {info.rows:>18,} rows │
└───────────────┴─────────────────────────┘"""


def format_bytes(size: int) -> str:
    """Convert bytes to a human-readable string."""

    if size <= 0:
        return "0 B"

    for unit in ("B", "KB", "MB", "GB", "TB", "PB", "EB", "ZB", "YB"):

        if size < 1024 or unit == "YB":

            if unit == "B":
                return f"{size} B"

            value = f"{size:.2f}".rstrip("0.")
            return f"{value} {unit}"

        size /= 1024


class DumpFormat(IntEnum):
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

    @property
    def memory_string(self) -> str:
        """Human-readable memory string."""

        return format_bytes(self.memory)

    @property
    def storage_string(self) -> str:
        """Human-readable storage string."""

        return format_bytes(self.storage)

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
