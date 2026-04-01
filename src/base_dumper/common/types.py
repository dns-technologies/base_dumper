from io import BufferedReader
from collections.abc import (
    Iterable,
    Generator,
)
from typing import (
    Any,
    Protocol,
    runtime_checkable,
)

from pandas import DataFrame as PdFrame
from polars import (
    DataFrame as PlFrame,
    LazyFrame as LfFrame,
)


@runtime_checkable
class CursorType(Protocol):
    """Protocol for Cursor object."""

    def execute(self, *args, **kwargs) -> None: ...
    def close(self, *args, **kwargs) -> None: ...


@runtime_checkable
class ReaderType(Protocol):
    """Protocol for Reader object."""

    fileobj: BufferedReader
    columns: list[str]
    dtypes: list[str]

    def to_bytes(self) -> Generator[bytes, None, None]: ...
    def to_rows(self) -> Generator[Any, None, None]: ...
    def to_pandas(self) -> PdFrame: ...
    def to_polars(self) -> PlFrame: ...
    def tell(self) -> int: ...
    def close(self) -> None: ...


@runtime_checkable
class WriterType(Protocol):
    """Protocol for Reader object."""

    metadata: object
    columns: list[str]
    dtypes: list[str]

    def from_bytes(self, bytes_data: Iterable[bytes]) -> int: ...
    def from_rows(
        self,
        rows: Iterable[list[Any] | tuple[Any, ...]],
    ) -> int: ...
    def from_pandas(self, data_frame: PdFrame) -> int: ...
    def from_polars(self, data_frame: PlFrame | LfFrame) -> int: ...
    def tell(self) -> int: ...
    def close(self) -> None: ...
