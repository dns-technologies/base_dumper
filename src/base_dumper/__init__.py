"""Abstract class and base common modules for dbhose-airflow dumpers."""

from light_compressor import CompressionMethod

from .common import (
    AbstractCursor,
    AbstractReader,
    BaseDumperError,
    BaseDumperTypeError,
    BaseDumperValueError,
    DBConnector,
    DBMetadata,
    DumperLogger,
    DumperMode,
    IsolationLevel,
    chunk_query,
    random_name,
    timeouts,
    transfer_diagram,
)
from .dumper import (
    BaseDumper,
    multiquery,
)
from .version import __version__

__all__ = (
    "__version__",
    "AbstractCursor",
    "AbstractReader",
    "BaseDumper",
    "BaseDumperError",
    "BaseDumperTypeError",
    "BaseDumperValueError",
    "CompressionMethod",
    "DBConnector",
    "DBMetadata",
    "DumperLogger",
    "DumperMode",
    "IsolationLevel",
    "chunk_query",
    "multiquery",
    "random_name",
    "timeouts",
    "transfer_diagram",
)
__author__ = "0xMihalich"
