from abc import (
    ABC,
    abstractmethod,
)
from collections import OrderedDict
from collections.abc import (
    Generator,
    Iterable,
)
from gc import collect
from io import (
    BufferedReader,
    BufferedWriter,
)
from logging import Logger
from types import MethodType
from typing import (
    Any,
    Optional,
)

from csvpack.common.sizes import CHUNK_SIZE
from light_compressor import (
    CompressionLevel,
    CompressionMethod,
    define_reader,
    define_writer,
)
from pandas import DataFrame as PdFrame
from polars import (
    DataFrame as PlFrame,
    LazyFrame as LfFrame,
)

from .common import (
    CursorType,
    DBConnector,
    DBMetadata,
    DumperLogger,
    DumperMode,
    DumpFormat,
    ReaderType,
    IsolationLevel,
    Timeout,
    STREAM_TYPE,
    chunk_query,
)
from .version import __version__


def multiquery(dump_method: MethodType):
    """Multiquery decorator."""

    def wrapper(*args, **kwargs):

        first_part: list[str]
        second_part: list[str]

        self: BaseDumper = args[0]
        dumper_src: BaseDumper = kwargs.get("dumper_src", self)
        queries: str = kwargs.get("query_src") or kwargs.get("query")
        part: int = 1
        first_part, second_part = chunk_query(queries)
        all_parts = len(sum((first_part, second_part), [])) or int(
            bool(kwargs.get("table_name") or kwargs.get("table_src"))
        )

        for query in first_part:
            self.logger.info(f"Execute query {part}/{all_parts}")
            dumper_src.mode_action(query)
            part += 1

        for key in ("query", "query_src"):
            if key in kwargs and second_part:
                kwargs[key] = second_part.pop(0)
                break

        self.logger.info(
            f"Execute stream {part}/{all_parts} [{self.stream_type} mode]"
        )
        try:
            yield self.mode_action(dump_method, *args, **kwargs)
        finally:
            collect()

            for query in second_part:
                part += 1
                self.logger.info(f"Execute query {part}/{all_parts}")
                dumper_src.mode_action(query)

    return wrapper


def chunk_bytes(fileobj: BufferedReader) -> Generator[bytes, None, None]:
    """Chunk fileobj generator."""

    while chunk := fileobj.read(CHUNK_SIZE):
        yield chunk


class BaseDumper(ABC):
    """Abstract dumper class."""

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
    dumper_version: str = __version__
    with_compression: bool = False
    is_between: bool = False

    def __init__(
        self,
        connector: DBConnector,
        compression_method: CompressionMethod = CompressionMethod.ZSTD,
        compression_level: int = CompressionLevel.ZSTD_DEFAULT,
        logger: Logger | None = None,
        timeout: int = Timeout.DBMS_1_HOUR_TIMEOUT_SEC,
        isolation: IsolationLevel = IsolationLevel.committed,
        mode: DumperMode = DumperMode.PROD,
        dump_format: DumpFormat = DumpFormat.BINARY,
        s3_file: bool = False,
    ) -> None:
        """Class initialization."""

        if not logger:
            logger_name = self.__class__.__name__
            version = self.dumper_version
            logger = DumperLogger(logger_name=logger_name, version=version)

        self.connector = connector
        self.compression_method = compression_method
        self.compression_level = compression_level
        self.logger = logger
        self.mode = mode
        self.dump_format = dump_format
        self.s3_file = s3_file
        self._timeout = timeout
        self._isolation = isolation

        if mode is not DumperMode.PROD:
            self.logger.warning(
                f"{self.__class__.__name__} run in {self.mode.name} mode.",
            )

        # Child dumper must be add initialize params and other settings after:
        # self.dumper_version = __version__
        # super().__init__(
        #     connector,
        #     compression_method,
        #     compression_level,
        #     logger,
        #     timeout,
        #     isolation,
        #     mode,
        #     dump_format,
        #     s3_file,
        # )
        # ... # <- child dumper __init__ code here

    def __enter__(self) -> "BaseDumper":
        """Context manager entry."""

        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc_val: BaseException | None,
        exc_tb: Any | None,
    ) -> None:
        """Context manager exit."""

        _ = exc_type, exc_val, exc_tb
        self.close()

    @property
    def stream_type(self) -> str:
        """Property method for get stream object type."""

        if self.dump_format is DumpFormat.BINARY:
            return STREAM_TYPE.get(self.dbname, self.dump_format.name.lower())

        return self.dump_format.name.lower()

    @property
    def timeout(self) -> int:
        """Property method for get statement_timeout."""

        return self._timeout

    @property
    def isolation(self) -> IsolationLevel:
        """Property method for get current
        server transaction isolation level."""

        return self._isolation

    def mode_action(
        self,
        action_data: str | MethodType | None = None,
        *args: Any,
        **kwargs: dict[str, Any],
    ) -> None:
        """DumperMode.DEBUG or DumperMode.TEST action.
        Default is do nothing."""

        if action_data:
            if isinstance(action_data, str):
                return self.cursor.execute(action_data)
            return action_data(*args, **kwargs)

    @abstractmethod
    def metadata(
        self,
        query: str | None = None,
        table_name: str | None = None,
    ) -> DBMetadata:
        """Read metadata from Server."""

    @multiquery
    @abstractmethod
    def _read_dump(
        self,
        fileobj: BufferedWriter,
        query: str | None,
        table_name: str | None,
    ) -> None:
        """Internal method read_dump for generate kwargs to decorator."""

    @multiquery
    def _write_between(
        self,
        table_dest: str,
        table_src: str | None,
        query_src: str | None,
        dumper_src: Optional["BaseDumper"],
    ) -> None:
        """Internal method write_between for generate kwargs to decorator."""

        if not dumper_src:
            dumper_src = self

        dumper_src.is_between = True
        source_compressed = dumper_src.with_compression
        destination_compressed = self.with_compression
        do_compress_read = source_compressed and not destination_compressed
        do_compress_write = ((not source_compressed and destination_compressed)
            or (source_compressed and destination_compressed and
            dumper_src.compression_method != self.compression_method))

        if (
            self.stream_type is dumper_src.stream_type
            and self.stream_type != "binary"
        ):
            self.from_fileobj(
                fileobj=dumper_src.to_fileobj(
                    query=query_src,
                    table_name=table_src,
                    do_compress_action=do_compress_read,
                ),
                table_name=table_dest,
                do_compress_action=do_compress_write,
            )
        else:
            source = dumper_src.metadata(
                    query=query_src,
                    table_name=table_src,
            )
            reader = dumper_src.to_reader(
                    query=query_src,
                    table_name=table_src,
            )
            dtype_data = reader.to_rows()
            self.from_rows(
                dtype_data=dtype_data,
                table_name=table_dest,
                source=source,
            )
            reader.close()
            collect()

        dumper_src.is_between = False

    @multiquery
    @abstractmethod
    def _to_reader(
        self,
        query: str | None,
        table_name: str | None,
    ) -> ReaderType:
        """Internal method to_reader for generate kwargs to decorator."""

    @multiquery
    @abstractmethod
    def _to_fileobj(
        self,
        query: str | None,
        table_name: str | None,
    ) -> BufferedReader:
        """Internal method to_fileobj for generate kwargs to decorator."""

    def read_dump(
        self,
        fileobj: BufferedWriter,
        query: str | None = None,
        table_name: str | None = None,
    ) -> None:
        """Read dump from Server."""

        return next(self._read_dump(
            fileobj=fileobj,
            query=query,
            table_name=table_name,
        ))

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
    ) -> None:
        """Write stream between Servers."""

        return next(self._write_between(
            table_dest=table_dest,
            table_src=table_src,
            query_src=query_src,
            dumper_src=dumper_src,
        ))

    def to_reader(
        self,
        query: str | None = None,
        table_name: str | None = None,
    ) -> ReaderType:
        """Get stream from Server as stream object."""

        return next(self._to_reader(
            query=query,
            table_name=table_name,
        ))

    def to_fileobj(
        self,
        query: str | None = None,
        table_name: str | None = None,
        compression_method: CompressionMethod | None = None,
        do_compress_action: bool = False,
    ) -> BufferedReader:
        """Get stream from Server as file object."""

        if not compression_method:
            compression_method = self.compression_method

        fileobj = next(self._to_fileobj(
            query=query,
            table_name=table_name,
        ))

        if do_compress_action:
            return define_reader(fileobj, compression_method)

        return fileobj

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
            dtype_data=data_frame.itertuples(index=False),
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
    def from_bytes(
        self,
        bytes_data: Iterable[bytes],
        table_name: str,
    ) -> None:
        """Write from iterable bytes into Server object."""

    def from_fileobj(
        self,
        fileobj: BufferedReader,
        table_name: str,
        compression_method: CompressionMethod | None = None,
        do_compress_action: bool = False,
    ) -> None:
        """Write from file object into Server object."""

        if not compression_method:
            compression_method = self.compression_method

        bytes_data = chunk_bytes(fileobj)

        if do_compress_action:
            bytes_data = define_writer(
                bytes_data,
                self.compression_method,
                self.compression_level,
            )

        self.from_bytes(bytes_data, table_name)

    @abstractmethod
    def refresh(self) -> None:
        """Refresh session."""

        self.logger.info(f"Connection to host {self.connector.host} updated.")

    def close(self) -> None:
        """Close session."""

        self.cursor.close()
        self.logger.info(f"Connection to host {self.connector.host} closed.")
