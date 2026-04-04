from collections.abc import Generator
from io import BufferedReader

from csvpack import (
    CSVPackReader,
    CSVPackMeta,
    CSVReader,
)
from csvpack.common.ptype import LIST
from csvpack.common.repr import table_repr
from csvpack.common.sizes import CHUNK_SIZE
from light_compressor import (
    CompressionMethod,
    define_reader,
)
from polars import Object

from .renders import DBMetadata


STREAM_TYPE = {
    "clickhouse": "native",
    "greenplum": "pgcopy",
    "postgres": "pgcopy",
    "sqlserver": "bcp",
    "oracle": "dmp",
}


class CSVStreamReader(CSVPackReader):
    """Class for manipulate uncompressed stream csv object."""

    db_metadata: DBMetadata
    fileobj: BufferedReader
    compression_method: CompressionMethod
    compression_stream: BufferedReader
    metadata: CSVPackMeta

    def __init__(
        self,
        metadata: DBMetadata,
        fileobj: BufferedReader,
        compression_method: CompressionMethod = CompressionMethod.NONE,
        delimiter: str = ",",
        quote_char: str = '"',
        encoding: str = "utf-8",
        has_header: bool = False,
    ) -> None:
        """Class initialization."""

        self.db_metadata = metadata
        self.fileobj = fileobj
        self.compression_method = compression_method
        self.compression_stream = define_reader(
            self.fileobj,
            self.compression_method,
        )
        self.metadata = CSVPackMeta.from_params(
            self.db_metadata.name,
            self.db_metadata.version,
            [column for column, _ in self.db_metadata.columns.items()],
            [dtype for _, dtype in self.db_metadata.columns.items()],
            delimiter=delimiter,
            quote_char=quote_char,
            encoding=encoding,
            has_header=has_header,
        )
        self.csv_reader = CSVReader(
            self.compression_stream,
            self.metadata.csv_metadata,
            self.metadata.delimiter,
            self.metadata.quote_char,
            self.metadata.encoding,
            self.metadata.has_header,
        )
        self.schema_overrides = {
            column: Object
            for columns in self.metadata.csv_metadata
            for column, ptype in columns.items()
            if LIST in ptype
        }

    def to_bytes(self) -> Generator[bytes, None, None]:
        """Get raw stream data."""

        while chunk := self.compression_stream.read(CHUNK_SIZE):
            yield chunk

    def __repr__(self) -> str:
        """String representation of CSVPackReader."""

        return table_repr(
            self.columns,
            self.dtypes,
            "<CSV stream reader>",
            [
                f"Total columns: {len(self.columns)}",
                f"Readed rows: {self.csv_reader.num_rows}",
                f"Source: {self.db_metadata.name}",
                f"Version: {self.db_metadata.version}",
            ],
        )
