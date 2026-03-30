import pytest
from base_dumper import (
    CompressionMethod,
    DumperMode,
    DumpFormat,
    IsolationLevel,
)


class TestCompressionMethod:
    """Тесты для CompressionMethod enum."""

    def test_values(self):
        assert CompressionMethod.NONE.value == 0x02  # noqa: S101
        assert CompressionMethod.LZ4.value == 0x82  # noqa: S101
        assert CompressionMethod.ZSTD.value == 0x90  # noqa: S101
        assert CompressionMethod.GZIP.value == 0x99  # noqa: S101
        assert CompressionMethod.SNAPPY.value == 0x9F  # noqa: S101

    def test_names(self):
        assert CompressionMethod.NONE.name == "NONE"  # noqa: S101
        assert CompressionMethod.LZ4.name == "LZ4"  # noqa: S101
        assert CompressionMethod.ZSTD.name == "ZSTD"  # noqa: S101
        assert CompressionMethod.GZIP.name == "GZIP"  # noqa: S101
        assert CompressionMethod.SNAPPY.name == "SNAPPY"  # noqa: S101

    def test_all_methods(self):
        methods = list(CompressionMethod)
        assert len(methods) == 5  # noqa: S101
        assert CompressionMethod.NONE in methods  # noqa: S101


class TestDumperMode:
    """Тесты для DumperMode enum."""

    def test_values(self):
        assert DumperMode.TEST.value == 0  # noqa: S101
        assert DumperMode.DEBUG.value == 1  # noqa: S101
        assert DumperMode.PROD.value == 2  # noqa: S101

    def test_names(self):
        assert DumperMode.TEST.name == "TEST"  # noqa: S101
        assert DumperMode.DEBUG.name == "DEBUG"  # noqa: S101
        assert DumperMode.PROD.name == "PROD"  # noqa: S101


class TestDumpFormat:
    """Тесты для DumpFormat enum."""

    def test_values(self):
        assert DumpFormat.binary.value == 0  # noqa: S101
        assert DumpFormat.csv.value == 1  # noqa: S101

    def test_names(self):
        assert DumpFormat.binary.name == "binary"  # noqa: S101
        assert DumpFormat.csv.name == "csv"  # noqa: S101


class TestIsolationLevel:
    """Тесты для IsolationLevel enum."""

    def test_values(self):
        assert IsolationLevel.uncommitted.value == "READ UNCOMMITTED"  # noqa: S101
        assert IsolationLevel.committed.value == "READ COMMITTED"  # noqa: S101
        assert IsolationLevel.repeatable.value == "REPEATABLE READ"  # noqa: S101
        assert IsolationLevel.serializable.value == "SERIALIZABLE"  # noqa: S101

    def test_names(self):
        assert IsolationLevel.uncommitted.name == "uncommitted"  # noqa: S101
        assert IsolationLevel.committed.name == "committed"  # noqa: S101
        assert IsolationLevel.repeatable.name == "repeatable"  # noqa: S101
        assert IsolationLevel.serializable.name == "serializable"  # noqa: S101


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
