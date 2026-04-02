from collections.abc import (
    Iterable,
    Generator,
)
from io import (
    BufferedReader,
    BufferedWriter,
)
from logging import Logger
from types import MethodType
from typing import (
    Any,
    Optional,
    Protocol,
    runtime_checkable,
)

from light_compressor import CompressionMethod
from pandas import DataFrame as PdFrame
from polars import (
    DataFrame as PlFrame,
    LazyFrame as LfFrame,
)

from .connector import DBConnector
from .diagram import DBMetadata
from .dump_format import DumpFormat
from .isolations import IsolationLevel
from .mode_level import DumperMode


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
    def from_rows(self, rows: Iterable[Iterable[Any]]) -> int: ...
    def from_pandas(self, data_frame: PdFrame) -> int: ...
    def from_polars(self, data_frame: PlFrame | LfFrame) -> int: ...
    def tell(self) -> int: ...
    def close(self) -> None: ...


@runtime_checkable
class DumperType(Protocol):
    """Protocol for Dumper object."""

    connector: DBConnector
    compression_method: CompressionMethod
    compression_level: int
    logger: Logger
    timeout: int
    isolation: IsolationLevel
    mode: DumperMode
    dump_format: DumpFormat
    dbmeta: DBMetadata | None
    cursor: CursorType
    dbname: str
    is_readonly: bool
    version: str
    dumper_version: str
    with_compression: bool = False
    is_between: bool = False

    @property
    def stream_type(self) -> str: ...
    @property
    def timeout(self) -> int: ...
    @property
    def isolation(self) -> IsolationLevel: ...
    def refresh(self) -> None: ...
    def close(self) -> None: ...

    def mode_action(
        self,
        action_data: str | MethodType | None = None,
        *args: Any,
        **kwargs: dict[str, Any],
    ) -> None: ...

    def metadata(
        self,
        query: str | None = None,
        table_name: str | None = None,
        reader_meta: bool = False,
    ) -> DBMetadata | object: ...

    def _read_dump(
        self,
        fileobj: BufferedWriter,
        query: str | None,
        table_name: str | None,
    ) -> None: ...

    def _write_between(
        self,
        table_dest: str,
        table_src: str | None,
        query_src: str | None,
        dumper_src: Optional["DumperType"],
    ) -> None: ...

    def _to_reader(
        self,
        query: str | None,
        table_name: str | None,
        metadata: DBMetadata | object | None = None,
    ) -> ReaderType: ...

    def _to_fileobj(
        self,
        query: str | None,
        table_name: str | None,
        metadata: DBMetadata | object | None = None,
    ) -> BufferedReader | DBMetadata: ...

    def write_dump(
        self,
        fileobj: BufferedReader,
        table_name: str,
    ) -> None: ...

    def write_between(
        self,
        table_dest: str,
        table_src: str | None = None,
        query_src: str | None = None,
        dumper_src: Optional["DumperType"] = None,
    ) -> None: ...

    def to_reader(
        self,
        query: str | None = None,
        table_name: str | None = None,
        metadata: DBMetadata | object | None = None,
    ) -> ReaderType: ...

    def to_fileobj(
        self,
        query: str | None = None,
        table_name: str | None = None,
        compression_method: CompressionMethod | None = None,
        do_compress_action: bool = False,
    ) -> BufferedReader: ...

    def from_rows(
        self,
        dtype_data: Iterable[Any],
        table_name: str,
        source: DBMetadata | object | None = None,
    ) -> None: ...

    def from_pandas(
        self,
        data_frame: PdFrame,
        table_name: str,
    ) -> None: ...

    def from_polars(
        self,
        data_frame: PlFrame | LfFrame,
        table_name: str,
    ) -> None: ...

    def from_bytes(
        self,
        bytes_data: Iterable[bytes],
        table_name: str,
        source: DBMetadata | object | None = None,
        destination: DBMetadata | None = None,
    ) -> None: ...

    def from_fileobj(
        self,
        fileobj: BufferedReader,
        table_name: str,
        compression_method: CompressionMethod | None = None,
        do_compress_action: bool = False,
        source: DBMetadata | object | None = None,
    ) -> None: ...
