from datetime import (
    datetime,
    date,
)
from decimal import Decimal
from uuid import uuid4

import pytest

from base_dumper.common import db_meta_from_iter


class TestDbMetaFromIter:
    """Тесты для db_meta_from_iter функции."""

    def test_simple_rows(self):
        """Test with simple rows of basic types."""

        rows = [
            (1, "Alice", 25, True, 50000.5),
            (2, "Bob", 30, False, 60000.0),
        ]
        dbmeta, generator = db_meta_from_iter(rows)
        assert dbmeta.name == "Iterable Object"  # noqa: S101
        assert "Python" in dbmeta.version  # noqa: S101
        assert len(dbmeta.columns) == 5  # noqa: S101
        assert dbmeta.columns["column_0"] == "int"  # noqa: S101
        assert dbmeta.columns["column_1"] == "str"  # noqa: S101
        assert dbmeta.columns["column_2"] == "int"  # noqa: S101
        assert dbmeta.columns["column_3"] == "bool"  # noqa: S101
        assert dbmeta.columns["column_4"] == "float"  # noqa: S101
        result = list(generator)
        assert len(result) == 2  # noqa: S101
        assert result[0][0] == 1  # noqa: S101
        assert result[0][1] == "Alice"  # noqa: S101

    def test_rows_with_none(self):
        """Test rows with None values."""

        rows = [
            (1, None, 25, True, 50000.5),
            (2, "Bob", 30, None, 60000.0),
            (3, "Alice", 35, False, 70000.0),
        ]
        dbmeta, generator = db_meta_from_iter(rows)
        assert dbmeta.columns["column_0"] == "int"  # noqa: S101
        assert (  # noqa: S101
            dbmeta.columns["column_1"] == "str"
        )
        assert dbmeta.columns["column_2"] == "int"  # noqa: S101
        assert dbmeta.columns["column_3"] == "bool"  # noqa: S101
        assert dbmeta.columns["column_4"] == "float"  # noqa: S101
        result = list(generator)
        assert len(result) == 3  # noqa: S101

    def test_all_none_rows(self):
        """Test when all values in first rows are None."""

        rows = [
            (None, None, None),
            (1, "Alice", 25),
            (2, "Bob", 30),
        ]
        dbmeta, generator = db_meta_from_iter(rows)
        assert dbmeta.columns["column_0"] == "int"  # noqa: S101
        assert dbmeta.columns["column_1"] == "str"  # noqa: S101
        assert dbmeta.columns["column_2"] == "int"  # noqa: S101
        result = list(generator)
        assert len(result) == 3  # noqa: S101

    def test_empty_iterator(self):
        """Test with empty iterator."""

        rows = []
        dbmeta, generator = db_meta_from_iter(rows)
        assert dbmeta.columns["column_0"] == "NoneType"  # noqa: S101
        result = list(generator)
        assert len(result) == 0  # noqa: S101

    def test_single_row(self):
        """Test with single row."""

        rows = [(42, "Answer", True)]
        dbmeta, generator = db_meta_from_iter(rows)
        assert dbmeta.columns["column_0"] == "int"  # noqa: S101
        assert dbmeta.columns["column_1"] == "str"  # noqa: S101
        assert dbmeta.columns["column_2"] == "bool"  # noqa: S101
        result = list(generator)
        assert len(result) == 1  # noqa: S101
        assert result[0][0] == 42  # noqa: S101

    def test_complex_types(self):
        """Test with complex Python types."""

        rows = [
            (
                datetime.now(),
                date.today(),
                uuid4(),
                Decimal("10.5"),
                ["a", "b"],
                {"key": "value"},
            ),
        ]
        dbmeta, generator = db_meta_from_iter(rows)
        assert dbmeta.columns["column_0"] == "datetime"  # noqa: S101
        assert dbmeta.columns["column_1"] == "date"  # noqa: S101
        assert dbmeta.columns["column_2"] == "UUID"  # noqa: S101
        assert dbmeta.columns["column_3"] == "Decimal"  # noqa: S101
        assert dbmeta.columns["column_4"] == "list"  # noqa: S101
        assert dbmeta.columns["column_5"] == "dict"  # noqa: S101
        result = list(generator)
        assert len(result) == 1  # noqa: S101

    def test_type_upgrade(self):
        """Test type upgrade when None is replaced with actual type."""

        rows = [
            (None, None),
            (1, "text"),
        ]
        dbmeta, generator = db_meta_from_iter(rows)
        assert dbmeta.columns["column_0"] == "int"  # noqa: S101
        assert dbmeta.columns["column_1"] == "str"  # noqa: S101
        result = list(generator)
        assert len(result) == 2  # noqa: S101

    def test_max_rows_limit(self):
        """Test max_rows parameter limits iteration."""

        rows = [(i, f"text_{i}") for i in range(200)]
        dbmeta, generator = db_meta_from_iter(rows, max_rows=10)
        assert dbmeta.columns["column_0"] == "int"  # noqa: S101
        assert dbmeta.columns["column_1"] == "str"  # noqa: S101
        result = list(generator)
        assert len(result) == 200  # noqa: S101

    def test_generator_preserves_order(self):
        """Test that generator preserves original row order."""

        rows = [
            (1, "first"),
            (2, "second"),
            (3, "third"),
        ]
        _, generator = db_meta_from_iter(rows)
        result = list(generator)
        assert result[0][1] == "first"  # noqa: S101
        assert result[1][1] == "second"  # noqa: S101
        assert result[2][1] == "third"  # noqa: S101

    def test_rows_with_different_types_same_column(self):
        """Test when same column has different types in different rows."""

        rows = [
            (1, "123"),
            (2, 456),
            (3, "789"),
        ]
        dbmeta, generator = db_meta_from_iter(rows)
        assert dbmeta.columns["column_1"] == "str"  # noqa: S101
        result = list(generator)
        assert len(result) == 3  # noqa: S101

    def test_nested_structures(self):
        """Test with nested lists and dicts."""

        rows = [
            ([1, 2, 3], {"a": 1, "b": 2}),
            ([4, 5], {"c": 3}),
        ]
        dbmeta, generator = db_meta_from_iter(rows)
        assert dbmeta.columns["column_0"] == "list"  # noqa: S101
        assert dbmeta.columns["column_1"] == "dict"  # noqa: S101
        result = list(generator)
        assert len(result) == 2  # noqa: S101
        assert result[0][0] == [1, 2, 3]  # noqa: S101
        assert result[0][1] == {"a": 1, "b": 2}  # noqa: S101

    def test_bytes_type(self):
        """Test with bytes type."""

        rows = [(b"binary_data",)]
        dbmeta, generator = db_meta_from_iter(rows)
        assert dbmeta.columns["column_0"] == "bytes"  # noqa: S101
        result = list(generator)
        assert result[0][0] == b"binary_data"  # noqa: S101


class TestDbMetaFromIterEdgeCases:
    """Edge cases tests."""

    def test_single_column(self):
        """Test with single column."""

        rows = [(1,), (2,), (3,)]
        dbmeta, generator = db_meta_from_iter(rows)
        assert len(dbmeta.columns) == 1  # noqa: S101
        assert dbmeta.columns["column_0"] == "int"  # noqa: S101
        result = list(generator)
        assert len(result) == 3  # noqa: S101

    def test_very_long_first_row(self):
        """Test with very long first row (many columns)."""

        rows = [tuple(range(100))]
        dbmeta, generator = db_meta_from_iter(rows)
        assert len(dbmeta.columns) == 100  # noqa: S101

        for i in range(100):
            assert dbmeta.columns[f"column_{i}"] == "int"  # noqa: S101

        result = list(generator)
        assert len(result) == 1  # noqa: S101

    def test_strings_with_different_types(self):
        """Test that strings remain strings even if they look like numbers."""

        rows = [("123",), ("456",), ("789",)]
        dbmeta, generator = db_meta_from_iter(rows)
        assert dbmeta.columns["column_0"] == "str"  # noqa: S101
        result = list(generator)
        assert result[0][0] == "123"  # noqa: S101


if __name__ == "__main__":
    pytest.main([__file__, "-svv"])
