# Base Dumper

Abstract base classes and common utilities for database dumpers in the `dbhose-airflow` ecosystem.

## Overview

`base_dumper` provides the foundation for building database dumpers with consistent interfaces for reading,

writing, and transferring data between different database systems.

It includes abstract classes, common data structures, and utility functions.

## Features

- **Abstract Base Classes**: `BaseDumper`, `AbstractCursor`, `ExampleReader`
- **Multi-query Support**: Automatic splitting and execution of multiple SQL statements
- **Data Format Support**: Convert between database tables and Python, pandas, or polars
- **Streaming**: Memory-efficient data transfer between sources
- **Transaction Management**: Configurable isolation levels
- **Logging**: Built-in logging with file and console output
- **Compression**: Integration with `light_compressor` for compressed dumps

## Installation

from developer pip

```bash
pip install base-dumper -U --index-url https://dns-technologies.github.io/dbhose-dev-pip/simple/
```

or from source

```bash
pip install . --extra-index-url https://dns-technologies.github.io/dbhose-dev-pip/simple/
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
)

class MyNewDumper(BaseDumper):
    def __init__(
        self,
        connector: DBConnector,
        compression_method: CompressionMethod,
        compression_level: int,
        logger: Logger | None,
        timeout: int,
        isolation: IsolationLevel,
        mode: DumperMode,
        dump_format: DumpFormat,
        s3_file: bool,
    ):
        self. __version__ = __version__
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
        # Implement MyNewDumper init
        ...

    # Implement abstract methods
    ...
```

### Data Transfer Methods

- **read_dump**: Read data from source to file
- **write_dump**: Write data from file to destination
- **write_between**: Transfer directly between databases
- **to_reader**: Get data as stream object
- **from_rows/pandas/polars**: Write from Python data structures

### Utilities

- **chunk_query**: Split multi-query strings into individual statements
- **transfer_diagram**: Generate visual representation of data transfer
- **random_name**: Generate random names for temporary objects
- **DBConnector**: Database connection parameters container

## Usage Example

```python
from base_dumper import DBConnector, CompressionMethod

# Create connector
connector = DBConnector(
    host="localhost",
    dbname="mydb",
    user="user",
    password="pass",
    port=5432,
)

# Initialize dumper (with concrete implementation)
dumper = MyNewDumper(
    connector=connector,
    compression_method=CompressionMethod.ZSTD,
    isolation=IsolationLevel.committed,
)

# Read dump to file
with open("dump.sql.zst", "wb") as f:
    dumper.read_dump(f, table_name="users")

# Convert to pandas DataFrame
reader = dumper.to_reader(table_name="users")
df = reader.to_pandas()
```

## Requirements

* Python >= 3.10
* light-compressor
* sqlparse
* pandas
* polars
