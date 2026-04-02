# Base Dumper

Abstract base classes and common utilities for database dumpers in the `dbhose-airflow` ecosystem.

## Overview

`base_dumper` provides the foundation for building database dumpers with consistent interfaces for reading, writing, and transferring data between different database systems.

It includes abstract classes, common data structures, and utility functions.

## Features

- **Abstract Base Classes** – `BaseDumper`, `CursorType`, `ReaderType`, `WriterType`
- **Multi-query Support** – Automatic splitting and execution of multiple SQL statements with `@multiquery` decorator
- **Dual Format Support** – BINARY (native database format) and CSV with automatic type conversion
- **Streaming** – Memory-efficient data transfer between sources
- **Transaction Management** – Configurable isolation levels
- **Logging** – Built-in logging with file and console output
- **Compression** – Integration with `light_compressor` for compressed dumps
- **S3 Mode** – Streaming upload support with tail storage
- **Pandas & Polars Integration** – Direct conversion from/to DataFrames
- **Visual Diagrams** – Table and transfer diagrams for debugging

## Installation

### From developer pip

```bash
pip install base-dumper -U --index-url https://dns-technologies.github.io/dbhose-dev-pip/simple/
```

### From source

```bash
pip install . --extra-index-url https://dns-technologies.github.io/dbhose-dev-pip/simple/
```

### From git

```bash
pip install git+https://github.com/dns-technologies/base_dumper --extra-index-url https://dns-technologies.github.io/dbhose-dev-pip/simple/
```

## Core Components

### BaseDumper

Abstract dumper class that all database-specific dumpers should inherit from.

```python
from logging import Logger
from base_dumper import (
    BaseDumper,
    CompressionMethod,
    DBConnector,
    DumperMode,
    DumpFormat,
    IsolationLevel,
    DBMetadata,
)


class MyNewDumper(BaseDumper):
    def __init__(
        self,
        connector: DBConnector,
        compression_method: CompressionMethod = CompressionMethod.ZSTD,
        compression_level: int = 3,
        logger: Logger | None = None,
        timeout: int = 3600,
        isolation: IsolationLevel = IsolationLevel.committed,
        mode: DumperMode = DumperMode.PROD,
        dump_format: DumpFormat = DumpFormat.BINARY,
        s3_file: bool = False,
    ) -> None:
        self.dumper_version = __version__
        super().__init__(
            connector,
            compression_method,
            compression_level,
            logger,
            timeout,
            isolation,
            mode,
            dump_format,
            s3_file,
        )
        # Child dumper initialization here

    # Required abstract methods implementation
    def metadata(self, query=None, table_name=None) -> DBMetadata: ...
    def _read_dump(self, fileobj, query=None, table_name=None) -> None: ...
    def _write_between(self, table_dest, table_src=None, query_src=None, dumper_src=None) -> None: ...
    def _to_reader(self, query=None, table_name=None) -> ReaderType: ...
    def _to_fileobj(self, query=None, table_name=None) -> BufferedReader: ...
    def write_dump(self, fileobj, table_name) -> None: ...
    def from_rows(self, rows, table_name, source=None) -> None: ...
    def from_bytes(self, bytes_data, table_name) -> None: ...
    def refresh(self) -> None: ...
```

### Data Transfer Methods

| Method | Description |
|--------|-------------|
| `read_dump(fileobj, query, table_name)` | Read data from source to file |
| `write_dump(fileobj, table_name)` | Write data from file to destination |
| `write_between(table_dest, table_src, query_src, dumper_src)` | Transfer directly between databases |
| `to_reader(query, table_name)` | Get data as stream reader object |
| `to_fileobj(query, table_name, compression_method, do_compress_action)` | Get data as file-like object |
| `from_rows(rows, table_name, source)` | Write from Python rows |
| `from_pandas(dataframe, table_name)` | Write from pandas DataFrame |
| `from_polars(dataframe, table_name)` | Write from polars DataFrame/LazyFrame |
| `from_bytes(bytes_data, table_name)` | Write from bytes chunks |
| `from_fileobj(fileobj, table_name, compression_method, do_compress_action)` | Write from file-like object |
| `refresh()` | Refresh database session |

### Reader Types

The package provides reader protocols that concrete implementations must follow:

**`ReaderType` Protocol** – Standard reader interface
- `to_rows()` – Returns generator of Python objects
- `to_pandas()` – Returns pandas DataFrame
- `to_polars(is_lazy)` – Returns polars DataFrame or LazyFrame
- `to_bytes()` – Returns generator of raw bytes chunks
- `tell()` – Returns current position
- `close()` – Closes the reader

### Writer Types

**`WriterType` Protocol** – Standard writer interface
- `from_rows(rows)` – Write from Python objects
- `from_pandas(df)` – Write from pandas DataFrame
- `from_polars(df)` – Write from polars DataFrame
- `from_bytes(bytes_data)` – Write from bytes chunks
- `tell()` – Returns current position
- `close()` – Closes the writer

### Utilities

| Function | Description |
|----------|-------------|
| `chunk_query(sql)` | Split multi-query string into individual statements |
| `get_query_kind(sql)` | Detect SQL query type (SELECT, INSERT, UPDATE, etc.) |
| `query_formatter(sql)` | Reformat SQL query |
| `transfer_diagram(source, destination)` | Generate visual representation of data transfer |
| `table_diagram(metadata)` | Generate visual representation of table schema |
| `log_diagram(logger, mode, source, destination)` | Log diagram to logger |
| `random_name()` | Generate random name for temporary objects |
| `DBConnector` | Database connection parameters container |
| `DBMetadata` | Database metadata container (name, version, columns) |
| `DumperLogger` | Built-in logger with file and console output |
| `STREAM_TYPE` | Mapping of database names to stream types (native, pgcopy, bcp) |

### Enums

| Enum | Values |
|------|--------|
| `CompressionMethod` | NONE, GZIP, LZ4, SNAPPY, ZSTD |
| `DumperMode` | TEST, DEBUG, PROD |
| `DumpFormat` | BINARY, CSV |
| `IsolationLevel` | uncommitted, committed, repeatable, serializable |

### Protocols

- `CursorType` – Protocol for database cursor objects (must implement `execute` and `close`)
- `ReaderType` – Protocol for reader objects (`to_rows`, `to_pandas`, `to_polars`, `to_bytes`, `tell`, `close`)
- `WriterType` – Protocol for writer objects (`from_rows`, `from_pandas`, `from_polars`, `from_bytes`, `tell`, `close`)

## Decorators

### @multiquery

Decorator for methods that need to execute multiple SQL queries before and after the main operation.

**Behavior:**
- Splits input SQL into first and second parts
- Executes first part queries before the main operation
- Executes second part queries after the main operation (in `finally` block)
- Works with methods that return generators for streaming

**Used in:**
- `_read_dump`
- `_write_between`
- `_to_reader`
- `_to_fileobj`

## Usage Examples

### Creating a Custom Dumper

```python
from base_dumper import BaseDumper, DBConnector, CompressionMethod, DumpFormat
from base_dumper import DBMetadata, ReaderType


class PostgreSQLDumper(BaseDumper):
    def metadata(self, query=None, table_name=None) -> DBMetadata:
        # Get column information from database
        self.cursor.execute(f"
            SELECT column_name, data_type
            FROM information_schema.columns
            WHERE table_name = '{table_name}'
        ")
        columns = {row[0]: row[1] for row in self.cursor.fetchall()}
        return DBMetadata(
            name=self.dbname,
            version=self.version,
            columns=columns,
        )

    def _read_dump(self, fileobj, query=None, table_name=None) -> None:
        # Use COPY TO STDOUT for binary format
        copy_cmd = f"COPY ({query or table_name}) TO STDOUT WITH (FORMAT binary)"
        self.cursor.copy_expert(copy_cmd, fileobj)

    def _write_between(self, table_dest, table_src=None, query_src=None, dumper_src=None) -> None:
        # Handle cross-database transfer
        reader = dumper_src._to_reader(query=query_src, table_name=table_src)
        self.from_rows(reader.to_rows(), table_name=table_dest)

    def _to_reader(self, query=None, table_name=None) -> ReaderType:
        # Return stream reader for binary format
        # Implementation depends on concrete reader class
        pass

    def _to_fileobj(self, query=None, table_name=None) -> BufferedReader:
        # Return raw file-like object
        # Implementation depends on concrete reader class
        pass

    def write_dump(self, fileobj, table_name) -> None:
        # Use COPY FROM STDIN for binary format
        copy_cmd = f"COPY {table_name} FROM STDIN WITH (FORMAT binary)"
        self.cursor.copy_expert(copy_cmd, fileobj)

    def from_rows(self, rows, table_name, source=None) -> None:
        # Convert rows to binary format and write
        writer = SomeBinaryWriter(None, self.pgtypes)
        self.from_bytes(writer.from_rows(rows), table_name)

    def from_bytes(self, bytes_data, table_name) -> None:
        # Write raw bytes to database
        copy_cmd = f"COPY {table_name} FROM STDIN WITH (FORMAT binary)"
        self.cursor.copy_expert(copy_cmd, bytes_data)

    def refresh(self) -> None:
        # Reconnect to database
        self.connect = Connection.connect(**self.connector._asdict())
        self.cursor = self.connect.cursor()
```

### Reading Data

```python
from base_dumper import DBConnector, CompressionMethod

connector = DBConnector(
    host="localhost",
    dbname="mydb",
    user="user",
    password="pass",
    port=5432,
)

# Assume we have a concrete dumper implementation
dumper = SomeDumper(
    connector=connector,
    compression_method=CompressionMethod.ZSTD,
    dump_format=DumpFormat.BINARY,
)

# Read to file
with open("dump.bin.zst", "wb") as f:
    dumper.read_dump(f, table_name="users")

# Get reader for streaming
reader = dumper.to_reader(table_name="users")
df = reader.to_pandas()
```

### Writing Data

```python
# Write from pandas
dumper.from_pandas(df, table_name="users")

# Write from Python rows
rows = [(1, "Alice"), (2, "Bob")]
dumper.from_rows(rows, table_name="users")
```

### Transfer Between Databases

```python
source_dumper = SomeDumper(connector=source_connector)
target_dumper = SomeDumper(connector=target_connector)

# Transfer directly
target_dumper.write_between(
    table_dest="users_copy",
    table_src="users",
    dumper_src=source_dumper,
)
```

### Multi-query Support

```python
# The @multiquery decorator automatically splits and executes queries
# Example SQL with multiple statements:
sql = "
    DROP TABLE IF EXISTS temp_users;
    CREATE TEMPORARY TABLE temp_users AS SELECT * FROM users;
    SELECT * FROM temp_users;
"

# First part (DROP, CREATE) executes before main operation
# Second part (SELECT) executes after main operation (in finally block)
dumper.read_dump(fileobj, query=sql)
```

### Debug Mode with Diagrams

```python
from base_dumper import DumperMode

dumper = SomeDumper(
    connector=connector,
    mode=DumperMode.DEBUG,
)

# This will log detailed diagrams showing:
# - Source table schema
# - Destination table schema
# - Transfer arrow diagram
dumper.write_between(table_dest="backup", table_src="users")
```

### Using CSVStreamReader

```python
from base_dumper import CSVStreamReader, DBMetadata
from collections import OrderedDict

# Create metadata with column types
columns = OrderedDict([
    ("id", "int4"),
    ("name", "text"),
    ("age", "int4"),
])

metadata = DBMetadata(
    name="postgres",
    version="14",
    columns=columns,
)

# Read CSV file with automatic type conversion
with open("data.csv", "rb") as f:
    reader = CSVStreamReader(f, metadata)
    df = reader.to_pandas()
```

## Requirements

- Python>=3.10
- csvpack==0.1.0.dev3
- light-compressor==0.1.1.dev1
- sqlparse==0.5.4
- pandas>=2.1.0
- polars>=0.20.31

## Dependencies Diagram

```text
base_dumper
├── csvpack (csv data support)
├── light_compressor (compression algorithms)
├── sqlparse (SQL parsing and splitting)
├── pandas (DataFrame support)
└── polars (DataFrame support)
```

*Note: Concrete dumper implementations (like `pgpack_dumper`) may have additional dependencies such as `psycopg` for PostgreSQL connectivity or `csvpack` for CSV format support.*

## License

MIT
