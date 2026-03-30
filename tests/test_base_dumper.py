import pytest
from unittest.mock import MagicMock
from base_dumper import (
    BaseDumper,
    DBConnector,
    CompressionMethod,
    DumperMode,
    DumpFormat,
    IsolationLevel,
)


class ConcreteDumper(BaseDumper):
    """Конкретная реализация для тестирования абстрактного класса."""

    def __init__(self, connector, **kwargs):
        super().__init__(connector, **kwargs)
        self._read_dump_calls = []
        self._write_between_calls = []
        self._to_reader_calls = []
        self._write_dump_calls = []
        self._from_rows_calls = []
        self._refresh_calls = []
        self._closed = False

    # Реализуем абстрактные методы из BaseDumper
    def _read_dump(self, fileobj, table_name=None, query=None, **kwargs):
        self._read_dump_calls.append(
            {
                "fileobj": fileobj,
                "table_name": table_name,
                "query": query,
                "kwargs": kwargs,
            }
        )
        return MagicMock()

    def _write_between(self, table_dest, table_src=None, **kwargs):
        self._write_between_calls.append(
            {
                "table_dest": table_dest,
                "table_src": table_src,
                "kwargs": kwargs,
            }
        )
        return MagicMock()

    def _to_reader(self, query=None, table_name=None, **kwargs):
        self._to_reader_calls.append(
            {
                "query": query,
                "table_name": table_name,
                "kwargs": kwargs,
            }
        )
        return MagicMock()

    def write_dump(self, fileobj, table_name=None, **kwargs):
        self._write_dump_calls.append(
            {
                "fileobj": fileobj,
                "table_name": table_name,
                "kwargs": kwargs,
            }
        )
        return MagicMock()

    def from_rows(self, rows, table_name=None, **kwargs):
        self._from_rows_calls.append(
            {
                "rows": rows,
                "table_name": table_name,
                "kwargs": kwargs,
            }
        )
        return MagicMock()

    def refresh(self, table_name=None, **kwargs):
        self._refresh_calls.append(
            {
                "table_name": table_name,
                "kwargs": kwargs,
            }
        )
        return MagicMock()

    def close(self):
        self._closed = True


@pytest.fixture
def base_connector():
    """Create base DBConnector for tests."""

    return DBConnector("localhost", "testdb", "user", "", 1234)


@pytest.fixture
def target_connector():
    """Create target DBConnector for tests."""

    return DBConnector("target", "targetdb", "user", "", 5678)


@pytest.fixture
def concrete_dumper(base_connector, mock_logger):
    """Create ConcreteDumper instance."""

    return ConcreteDumper(
        connector=base_connector,
        compression_method=CompressionMethod.ZSTD,
        compression_level=3,
        logger=mock_logger,
        timeout=30,
        isolation=IsolationLevel.committed,
        mode=DumperMode.PROD,
        dump_format=DumpFormat.binary,
    )


class TestAbstractMethods:
    """Тесты для абстрактных методов."""

    def test_instantiation(self, concrete_dumper):
        """Test that concrete dumper can be instantiated."""

        assert concrete_dumper is not None  # noqa: S101

    def test_methods_exist(self, concrete_dumper):
        """Test that all abstract methods are implemented."""

        methods = [
            "_read_dump",
            "_write_between",
            "_to_reader",
            "write_dump",
            "from_rows",
            "refresh",
        ]
        for method in methods:
            assert hasattr(concrete_dumper, method)  # noqa: S101
            assert callable(getattr(concrete_dumper, method))  # noqa: S101

    def test_methods_can_be_called(self, concrete_dumper: ConcreteDumper):
        """Test that methods can be called without errors."""

        # Просто вызываем методы с None аргументами
        concrete_dumper._read_dump(None)
        concrete_dumper._write_between(None)
        concrete_dumper._to_reader(None)
        concrete_dumper.write_dump(None)
        concrete_dumper.from_rows(None)
        concrete_dumper.refresh(None)


class TestBaseDumperInitialization:
    """Тесты инициализации BaseDumper."""

    def test_init_with_all_params(self, base_connector, mock_logger):
        """Test initialization with all parameters."""
        dumper = ConcreteDumper(
            connector=base_connector,
            compression_method=CompressionMethod.GZIP,
            compression_level=5,
            logger=mock_logger,
            timeout=60,
            isolation=IsolationLevel.repeatable,
            mode=DumperMode.DEBUG,
            dump_format=DumpFormat.csv,
        )
        assert dumper.connector == base_connector  # noqa: S101
        assert dumper.compression_method == CompressionMethod.GZIP  # noqa: S101
        assert dumper.compression_level == 5  # noqa: S101
        assert dumper.logger == mock_logger  # noqa: S101
        assert dumper.timeout == 60  # noqa: S101
        assert dumper.isolation == IsolationLevel.repeatable  # noqa: S101
        assert dumper.mode == DumperMode.DEBUG  # noqa: S101
        assert dumper.dump_format == DumpFormat.csv  # noqa: S101

    def test_init_with_defaults(self, base_connector, mock_logger):
        """Test initialization with defaults."""

        dumper = ConcreteDumper(connector=base_connector, logger=mock_logger)
        assert dumper.compression_method == CompressionMethod.ZSTD  # noqa: S101
        assert dumper.compression_level == 3  # noqa: S101
        assert dumper.timeout == 3600  # noqa: S101
        assert dumper.isolation == IsolationLevel.committed  # noqa: S101
        assert dumper.mode == DumperMode.PROD  # noqa: S101
        assert dumper.dump_format == DumpFormat.binary  # noqa: S101

    def test_init_without_connector(self):
        """Test initialization without connector."""

        with pytest.raises(TypeError):
            ConcreteDumper()  # type: ignore


class TestBaseDumperRead:
    """Тесты методов чтения."""

    def test_read_dump_calls__read_dump(
        self,
        concrete_dumper: ConcreteDumper,
        sample_fileobj,
    ):
        """Test that read_dump calls _read_dump."""

        concrete_dumper.read_dump(sample_fileobj, table_name="users")  # noqa: S101
        assert len(concrete_dumper._read_dump_calls) == 1  # noqa: S101
        call = concrete_dumper._read_dump_calls[0]  # noqa: S101
        assert call["fileobj"] == sample_fileobj  # noqa: S101
        assert call["table_name"] == "users"  # noqa: S101
        assert call["query"] is None  # noqa: S101

    def test_read_dump_with_query(
        self,
        concrete_dumper: ConcreteDumper,
        sample_fileobj,
    ):
        """Test read_dump with custom query."""

        query = "SELECT * FROM users WHERE id > 100"
        concrete_dumper.read_dump(sample_fileobj, query=query)
        call = concrete_dumper._read_dump_calls[0]
        assert call["query"] == query  # noqa: S101

    def test_read_dump_with_kwargs(
        self,
        concrete_dumper: ConcreteDumper,
        sample_fileobj,
    ):
        """Test read_dump with additional kwargs."""

        concrete_dumper.read_dump(
            sample_fileobj,
            query="select 1",
            table_name="users",
        )
        call = concrete_dumper._read_dump_calls[0]
        assert call["query"] == "select 1"  # noqa: S101
        assert call["table_name"] == "users"  # noqa: S101


class TestBaseDumperWriteBetween:
    """Тесты методов передачи между базами."""

    def test_write_between_calls__write_between(
        self,
        concrete_dumper: ConcreteDumper,
    ):
        """Test that write_between calls _write_between."""

        concrete_dumper.write_between(table_dest="employee", table_src="users")
        assert len(concrete_dumper._write_between_calls) == 1  # noqa: S101
        call = concrete_dumper._write_between_calls[0]
        assert call["table_dest"] == "employee"  # noqa: S101
        assert call["table_src"] == "users"  # noqa: S101

    def test_write_between_with_kwargs(self, concrete_dumper: ConcreteDumper):
        """Test write_between with additional kwargs."""

        concrete_dumper._write_between(
            table_dest="users",
            batch_size=1000,
        )
        call = concrete_dumper._write_between_calls[0]
        assert call["kwargs"]["batch_size"] == 1000  # noqa: S101


class TestBaseDumperToReader:
    """Тесты методов to_reader."""

    def test_to_reader_calls__to_reader(self, concrete_dumper: ConcreteDumper):
        """Test that to_reader calls _to_reader."""

        concrete_dumper.to_reader(table_name="users")
        assert len(concrete_dumper._to_reader_calls) == 1  # noqa: S101
        call = concrete_dumper._to_reader_calls[0]
        assert call["table_name"] == "users"  # noqa: S101


class TestBaseDumperWriteDump:
    """Тесты методов write_dump."""

    def test_write_dump_calls_write_dump(
        self, concrete_dumper: ConcreteDumper, sample_fileobj
    ):
        """Test that write_dump is called."""

        concrete_dumper.write_dump(sample_fileobj, table_name="users")
        assert len(concrete_dumper._write_dump_calls) == 1  # noqa: S101
        call = concrete_dumper._write_dump_calls[0]
        assert call["fileobj"] == sample_fileobj  # noqa: S101
        assert call["table_name"] == "users"  # noqa: S101

    def test_write_dump_with_kwargs(
        self, concrete_dumper: ConcreteDumper, sample_fileobj
    ):
        """Test write_dump with additional kwargs."""

        concrete_dumper.write_dump(
            sample_fileobj,
            table_name="users",
            if_exists="replace",
        )
        call = concrete_dumper._write_dump_calls[0]
        assert call["kwargs"]["if_exists"] == "replace"  # noqa: S101


class TestBaseDumperFromRows:
    """Тесты методов from_rows."""

    def test_from_rows_calls_from_rows(self, concrete_dumper: ConcreteDumper):
        """Test that from_rows is called."""

        rows = [(1, "test"), (2, "test2")]
        concrete_dumper.from_rows(rows, table_name="users")
        assert len(concrete_dumper._from_rows_calls) == 1  # noqa: S101
        call = concrete_dumper._from_rows_calls[0]
        assert call["table_name"] == "users"  # noqa: S101

    def test_from_rows_with_generator(self, concrete_dumper: ConcreteDumper):
        """Test from_rows with generator."""

        def row_generator():
            for i in range(10):
                yield (i, f"test_{i}")

        concrete_dumper.from_rows(row_generator(), table_name="users")
        assert len(concrete_dumper._from_rows_calls) == 1  # noqa: S101


class TestBaseDumperProperties:
    """Тесты свойств BaseDumper."""

    def test_version_property(self, concrete_dumper: ConcreteDumper):
        """Test that version is defined."""

        assert isinstance(concrete_dumper.__version__, str)  # noqa: S101
        assert len(concrete_dumper.__version__) > 0  # noqa: S101

    def test_dumper_has_logger(self, concrete_dumper: ConcreteDumper):
        """Test that dumper has logger attribute."""

        assert concrete_dumper.logger is not None  # noqa: S101

    def test_dumper_has_connector(self, concrete_dumper: ConcreteDumper):
        """Test that dumper has connector attribute."""

        assert concrete_dumper.connector is not None  # noqa: S101

    def test_stream_type_binary(self, concrete_dumper: ConcreteDumper):
        """Test stream_type returns correct binary
        format for any databases."""

        concrete_dumper.dump_format = DumpFormat.binary
        # PostgreSQL
        concrete_dumper.dbname = "postgres"
        assert concrete_dumper.stream_type == "pgcopy"  # noqa: S101
        # ClickHouse
        concrete_dumper.dbname = "clickhouse"
        assert concrete_dumper.stream_type == "native"  # noqa: S101
        # SQL Server
        concrete_dumper.dbname = "sqlserver"
        assert concrete_dumper.stream_type == "bcp"  # noqa: S101
        # Greenplum
        concrete_dumper.dbname = "greenplum"
        assert concrete_dumper.stream_type == "pgcopy"  # noqa: S101
        # Unknown
        concrete_dumper.dbname = "duckdb"
        assert concrete_dumper.stream_type == "binary"  # noqa: S101

    def test_stream_type_csv(self, concrete_dumper: ConcreteDumper):
        """Test stream_type returns csv format for any databases."""

        concrete_dumper.dump_format = DumpFormat.csv
        # PostgreSQL
        concrete_dumper.dbname = "postgres"
        assert concrete_dumper.stream_type == "csv"  # noqa: S101
        # ClickHouse
        concrete_dumper.dbname = "clickhouse"
        assert concrete_dumper.stream_type == "csv"  # noqa: S101
        # SQL Server
        concrete_dumper.dbname = "sqlserver"
        assert concrete_dumper.stream_type == "csv"  # noqa: S101
        # Greenplum
        concrete_dumper.dbname = "greenplum"
        assert concrete_dumper.stream_type == "csv"  # noqa: S101
        # Unknown
        concrete_dumper.dbname = "duckdb"
        assert concrete_dumper.stream_type == "csv"  # noqa: S101


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
