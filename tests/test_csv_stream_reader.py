from collections import OrderedDict
import gzip
import io

import pytest

from base_dumper import (
    CompressionMethod,
    CSVStreamReader,
    DBMetadata,
)


@pytest.fixture
def sample_db_metadata():
    """Create sample DBMetadata for tests."""

    columns = OrderedDict(
        [
            ("id", "int4"),
            ("name", "text"),
            ("age", "int4"),
            ("active", "bool"),
            ("salary", "float8"),
            ("created_date", "date"),
            ("created_datetime", "timestamp"),
            ("tags", "_text"),
        ]
    )
    return DBMetadata(
        name="postgres",
        version="14",
        columns=columns,
    )


@pytest.fixture
def sample_csv_data():
    """Create sample CSV data as bytes."""

    return (
        b"1,Alice,25,True,50000.5,2024-01-01,2024-01-01 10:00:00,\"['python','data']\"\n"  # noqa: E501
        b"2,Bob,30,False,60000.0,2024-01-02,2024-01-02 10:00:00,\"['rust','csv']\"\n"  # noqa: E501
        b"3,Charlie,35,True,75000.75,2024-01-03,2024-01-03 10:00:00,\"['pandas']\"\n"  # noqa: E501
        b"4,Diana,28,False,48000.25,2024-01-04,2024-01-04 10:00:00,[]\n"
        b"5,Eve,32,True,82000.0,2024-01-05,2024-01-05 10:00:00,\"['numpy','polars']\"\n"  # noqa: E501
    )


@pytest.fixture
def sample_csv_buffer(sample_csv_data):
    """Create buffer with sample CSV data."""

    buffer = io.BytesIO(sample_csv_data)
    buffer.seek(0)
    return buffer


class TestCSVStreamReader:
    """Tests for CSVStreamReader."""

    def test_reader_initialization(
        self, sample_db_metadata, sample_csv_buffer
    ):
        """Test CSVStreamReader initialization."""
        reader = CSVStreamReader(
            fileobj=sample_csv_buffer,
            metadata=sample_db_metadata,
            compression_method=CompressionMethod.NONE,
        )

        assert reader.fileobj is not None  # noqa: S101
        assert reader.db_metadata == sample_db_metadata  # noqa: S101
        assert reader.compression_method == CompressionMethod.NONE  # noqa: S101
        assert reader.columns == [  # noqa: S101
            "id",
            "name",
            "age",
            "active",
            "salary",
            "created_date",
            "created_datetime",
            "tags",
        ]
        assert len(reader.dtypes) == 8  # noqa: S101
        reader.close()

    def test_to_rows(self, sample_db_metadata, sample_csv_buffer):
        """Test reading rows from CSVStreamReader."""

        reader = CSVStreamReader(
            fileobj=sample_csv_buffer,
            metadata=sample_db_metadata,
            compression_method=CompressionMethod.NONE,
        )
        rows = list(reader.to_rows())
        assert len(rows) == 5  # noqa: S101
        assert rows[0][0] == 1  # id  # noqa: S101
        assert rows[0][1] == "Alice"  # name  # noqa: S101
        assert rows[0][2] == 25  # age  # noqa: S101
        assert rows[0][3] is True  # active  # noqa: S101
        assert rows[0][4] == 50000.5  # salary  # noqa: S101
        reader.close()

    def test_to_pandas(self, sample_db_metadata, sample_csv_buffer):
        """Test converting to pandas DataFrame."""

        reader = CSVStreamReader(
            fileobj=sample_csv_buffer,
            metadata=sample_db_metadata,
            compression_method=CompressionMethod.NONE,
        )
        df = reader.to_pandas()
        assert len(df) == 5  # noqa: S101
        assert list(df.columns) == [  # noqa: S101
            "id",
            "name",
            "age",
            "active",
            "salary",
            "created_date",
            "created_datetime",
            "tags",
        ]
        assert df["id"].iloc[0] == 1  # noqa: S101
        assert df["name"].iloc[0] == "Alice"  # noqa: S101
        reader.close()

    def test_to_polars(self, sample_db_metadata, sample_csv_buffer):
        """Test converting to polars DataFrame."""

        reader = CSVStreamReader(
            fileobj=sample_csv_buffer,
            metadata=sample_db_metadata,
            compression_method=CompressionMethod.NONE,
        )
        df = reader.to_polars()
        assert len(df) == 5  # noqa: S101
        assert df.columns == [  # noqa: S101
            "id",
            "name",
            "age",
            "active",
            "salary",
            "created_date",
            "created_datetime",
            "tags",
        ]
        assert df["id"][0] == 1  # noqa: S101
        assert df["name"][0] == "Alice"  # noqa: S101
        reader.close()

    def test_to_bytes(
        self, sample_db_metadata, sample_csv_buffer, sample_csv_data
    ):
        """Test reading raw bytes from CSVStreamReader."""

        reader = CSVStreamReader(
            fileobj=sample_csv_buffer,
            metadata=sample_db_metadata,
            compression_method=CompressionMethod.NONE,
        )

        chunks = list(reader.to_bytes())
        assert len(chunks) > 0  # noqa: S101
        full_data = b"".join(chunks)  # noqa: S101
        assert full_data == sample_csv_data  # noqa: S101
        reader.close()

    def test_no_compression(self, sample_db_metadata, sample_csv_buffer):
        """Test CSVStreamReader with no compression (default)."""

        reader = CSVStreamReader(
            fileobj=sample_csv_buffer,
            metadata=sample_db_metadata,
        )

        rows = list(reader.to_rows())
        assert len(rows) == 5  # noqa: S101
        assert rows[0][0] == 1  # noqa: S101
        reader.close()


class TestCSVStreamReaderEdgeCases:
    """Edge cases tests for CSVStreamReader."""

    def test_empty_file(self, sample_db_metadata):
        """Test CSVStreamReader with empty file."""

        empty_buffer = io.BytesIO(b"")
        empty_buffer.seek(0)
        reader = CSVStreamReader(
            fileobj=empty_buffer,
            metadata=sample_db_metadata,
            compression_method=CompressionMethod.NONE,
        )
        rows = list(reader.to_rows())
        assert len(rows) == 0  # noqa: S101
        reader.close()

    def test_repr(self, sample_db_metadata, sample_csv_buffer):
        """Test string representation of CSVStreamReader."""

        reader = CSVStreamReader(
            fileobj=sample_csv_buffer,
            metadata=sample_db_metadata,
            compression_method=CompressionMethod.NONE,
        )
        list(reader.to_rows())
        repr_str = repr(reader)
        assert "<CSV stream reader>" in repr_str  # noqa: S101
        assert "Total columns: 8" in repr_str  # noqa: S101
        assert "Source: postgres" in repr_str  # noqa: S101
        assert "Version: 14" in repr_str  # noqa: S101
        reader.close()


class TestCSVStreamReaderWithGzip:
    """Tests for CSVStreamReader with compression."""

    @pytest.fixture
    def gzip_compressed_csv(self, sample_csv_data):
        """Create gzip compressed CSV data."""

        buffer = io.BytesIO()
        with gzip.GzipFile(fileobj=buffer, mode="wb") as gz:
            gz.write(sample_csv_data)
        buffer.seek(0)
        return buffer

    def test_with_gzip_compression(
        self, sample_db_metadata, gzip_compressed_csv
    ):
        """Test CSVStreamReader with gzip compression."""

        reader = CSVStreamReader(
            fileobj=gzip_compressed_csv,
            metadata=sample_db_metadata,
            compression_method=CompressionMethod.GZIP,
        )
        rows = list(reader.to_rows())
        assert len(rows) == 5  # noqa: S101
        assert rows[0][0] == 1  # noqa: S101
        assert rows[0][1] == "Alice"  # noqa: S101
        reader.close()


if __name__ == "__main__":
    pytest.main([__file__, "-svv"])
