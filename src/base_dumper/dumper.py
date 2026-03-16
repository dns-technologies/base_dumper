from abc import (
    ABC,
    abstractmethod,
)
from collections import OrderedDict
from gc import collect
from io import (
    BufferedReader,
    BufferedWriter,
)
from logging import Logger
from types import MethodType
from typing import (
    Any,
    Iterable,
    Optional,
)

from light_compressor import (
    CompressionLevel,
    CompressionMethod,
)
from pandas import DataFrame as PdFrame
from polars import (
    DataFrame as PlFrame,
    LazyFrame as LfFrame,
)

from .common import (
    AbstractCursor,
    ExampleReader,
    DBConnector,
    DBMetadata,
    DumperLogger,
    DumperMode,
    IsolationLevel,
    chunk_query,
    timeouts,
)
from .version import __version__


def multiquery(dump_method: MethodType):
    """Multiquery decorator."""

    def wrapper(*args, **kwargs):

        first_part: list[str]
        second_part: list[str]

        self: BaseDumper = args[0]
        cursor: AbstractCursor = (kwargs.get("dumper_src") or self).cursor
        query: str = kwargs.get("query_src") or kwargs.get("query")
        part: int = 1
        first_part, second_part = chunk_query(query)
        all_parts = len(sum((first_part, second_part), [])) or int(
            bool(kwargs.get("table_name") or kwargs.get("table_src"))
        )

        for query in first_part:
            self.logger.info(f"Execute query {part}/{all_parts}")
            cursor.execute(query)
            part += 1

        for key in ("query", "query_src"):
            if key in kwargs and second_part:
                kwargs[key] = second_part.pop(0)
                break

        self.logger.info(
            f"Execute stream {part}/{all_parts} [{self.stream_type} mode]"
        )
        output = dump_method(*args, **kwargs)

        if output:
            self.refresh()

        cursor: AbstractCursor = (kwargs.get("dumper_src") or self).cursor
        collect()

        for query in second_part:
            part += 1
            self.logger.info(f"Execute query {part}/{all_parts}")
            cursor.execute(query)

        return output

    return wrapper


class BaseDumper(ABC):
    """Abstract dumper class."""

    connector: DBConnector
    compression_method: CompressionMethod
    compression_level: int
    logger: Logger
    timeout: int
    isolation: IsolationLevel
    mode: DumperMode
    dbmeta: DBMetadata | None
    cursor: AbstractCursor
    dbname: str
    is_readonly: bool
    stream_type: str
    version: str = __version__

    def __init__(
        self,
        connector: DBConnector,
        compression_method: CompressionMethod = CompressionMethod.ZSTD,
        compression_level: int = CompressionLevel.DEFAULT_COMPRESSION,
        logger: Logger | None = None,
        timeout: int = timeouts.DBMS_1_HOUR_TIMEOUT_SEC,
        isolation: IsolationLevel = IsolationLevel.committed,
        mode: DumperMode = DumperMode.PROD,
    ) -> None:
        """Class initialization."""

        if not logger:
            logger_name = self.__class__.__name__
            version=self.version
            logger = DumperLogger(logger_name=logger_name, version=version)

        self.connector = connector
        self.compression_method = compression_method
        self.compression_level = compression_level
        self.logger = logger
        self.mode = mode
        self._timeout = timeout
        self._isolation = isolation

        # Child dumper must be add initialize params and other settings after:
        # super().__init__(
        #     connector,
        #     compression_method,
        #     compression_level,
        #     logger,
        #     timeout,
        #     isolation,
        #     mode,
        # )
        # ... # <- child dumper __init__ code here

    @property
    def timeout(self) -> int:
        """Property method for get statement_timeout."""

        return self._timeout

    @property
    def isolation(self) -> IsolationLevel:
        """Property method for get current
        server transaction isolation level."""

        return self._isolation

    @multiquery
    @abstractmethod
    def _read_dump(
        self,
        fileobj: BufferedWriter,
        query: str | None,
        table_name: str | None,
    ) -> bool:
        """Internal method read_dump for generate kwargs to decorator."""

    @multiquery
    @abstractmethod
    def _write_between(
        self,
        table_dest: str,
        table_src: str | None,
        query_src: str | None,
        dumper_src: Optional["BaseDumper"],
    ) -> bool:
        """Internal method write_between for generate kwargs to decorator."""

    @multiquery
    @abstractmethod
    def _to_reader(
        self,
        query: str | None,
        table_name: str | None,
    ) -> ExampleReader:
        """Internal method to_reader for generate kwargs to decorator."""

    def read_dump(
        self,
        fileobj: BufferedWriter,
        query: str | None = None,
        table_name: str | None = None,
    ) -> bool:
        """Read dump from Server."""

        return self._read_dump(
            fileobj=fileobj,
            query=query,
            table_name=table_name,
        )

    @abstractmethod
    def write_dump(
        self,
        fileobj: BufferedReader,
        table_name: str,
    ) -> None:
        """Write dump into Server."""

    def write_between(
        self,
        table_dest: str,
        table_src: str | None = None,
        query_src: str | None = None,
        dumper_src: Optional["BaseDumper"] = None,
    ) -> bool:
        """Write stream between Servers."""

        return self._write_between(
            table_dest=table_dest,
            table_src=table_src,
            query_src=query_src,
            dumper_src=dumper_src,
        )

    def to_reader(
        self,
        query: str | None = None,
        table_name: str | None = None,
    ) -> ExampleReader:
        """Get stream from Server as stream object."""

        return self._to_reader(
            query=query,
            table_name=table_name,
        )

    @abstractmethod
    def from_rows(
        self,
        dtype_data: Iterable[Any],
        table_name: str,
        source: DBMetadata | None = None,
    ) -> None:
        """Write from python list into Server object."""

    def from_pandas(
        self,
        data_frame: PdFrame,
        table_name: str,
    ) -> None:
        """Write from pandas.DataFrame into Server object."""

        self.from_rows(
            dtype_data=iter(data_frame.values),
            table_name=table_name,
            source=DBMetadata(
                name="pandas",
                version="DataFrame",
                columns=OrderedDict(zip(
                    data_frame.columns,
                    [str(dtype) for dtype in data_frame.dtypes],
                )),
            )
        )

    def from_polars(
        self,
        data_frame: PlFrame | LfFrame,
        table_name: str,
    ) -> None:
        """Write from polars.DataFrame/LazyFrame into Server object."""

        if data_frame.__class__ is LfFrame:
            data_frame = data_frame.collect(engine="streaming")
            version = "LazyFrame"
        elif data_frame.__class__ is PlFrame:
            version = "DataFrame"
        else:
            raise ValueError("data_frame is not a Polars.DataFrame/LazyFrame")

        self.from_rows(
            dtype_data=data_frame.iter_rows(),
            table_name=table_name,
            source=DBMetadata(
                name="polars",
                version=version,
                columns=OrderedDict(zip(
                    data_frame.columns,
                    [str(dtype) for dtype in data_frame.dtypes],
                )),
            )
        )

    @abstractmethod
    def refresh(self) -> None:
        """Refresh session."""

        self.logger.info(f"Connection to host {self.connector.host} updated.")

    def close(self) -> None:
        """Close session."""

        self.cursor.close()
        self.logger.info(f"Connection to host {self.connector.host} closed.")
