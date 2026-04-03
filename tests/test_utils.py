from collections import OrderedDict

import pytest
from base_dumper import (
    DBMetadata,
    DumperMode,
    chunk_query,
    random_name,
    transfer_table,
    single_table,
    log_table,
)


class TestChunkQuery:
    """Тесты для chunk_query функции."""

    def test_single_query(self):
        """Test splitting single query."""

        sql = "SELECT * FROM users;"
        result = chunk_query(sql)
        assert len(result) == 2  # noqa: S101
        assert result[0] == []  # noqa: S101
        assert result[1] == ["SELECT * FROM users"]  # noqa: S101

    def test_multiple_queries(self):
        """Test splitting multiple queries."""

        sql = "SELECT * FROM users; SELECT * FROM orders;"
        result = chunk_query(sql)
        assert len(result) == 2  # noqa: S101
        assert "SELECT * FROM users" in result[0]  # noqa: S101
        assert "SELECT * FROM orders" in result[1]  # noqa: S101

    def test_queries_with_newline(self):
        """Test queries separated by newline."""

        sql = "SELECT * FROM users;\nSELECT * FROM orders;"
        result = chunk_query(sql)
        assert len(result) == 2  # noqa: S101

    def test_query_with_semicolon_in_string(self):
        """Test semicolon inside string literal."""

        sql = "SELECT * FROM users WHERE name = 'O''Reilly'; SELECT 1;"
        result = chunk_query(sql)
        assert len(result) == 2  # noqa: S101
        assert "O''Reilly" in result[0][0]  # noqa: S101

    def test_query_with_semicolon_in_quotes(self):
        """Test semicolon inside quoted string."""

        sql = 'SELECT * FROM users WHERE name = "O;Reilly"; SELECT 1;'
        result = chunk_query(sql)
        assert len(result) == 2  # noqa: S101

    def test_empty_sql(self):
        """Test empty SQL string."""

        result = chunk_query("")
        assert len(result) == 2  # noqa: S101

    def test_no_trailing_semicolon(self):
        """Test query without trailing semicolon."""

        sql = "SELECT * FROM users"
        result = chunk_query(sql)
        assert len(result) == 2  # noqa: S101
        assert result[1][0] == "SELECT * FROM users"  # noqa: S101

    def test_sql_with_comments(self):
        """Test SQL with comments."""
        sql = "-- Comment\nSELECT * FROM users; SELECT 1;"
        result = list(chunk_query(sql))
        assert len(result) == 2  # noqa: S101

    def test_sql_with_multiline_comment(self):
        """Test SQL with multiline comments."""

        sql = "/* Comment */ SELECT * FROM users; SELECT 1;"
        result = chunk_query(sql)
        assert len(result) == 2  # noqa: S101

    def test_large_sql(self):
        """Test large SQL string."""

        queries = [f"SELECT {i};" for i in range(100)]
        sql = " ".join(queries)
        result = chunk_query(sql)
        assert len(result) == 2  # noqa: S101
        assert len(result[0]) == 99  # noqa: S101
        assert len(result[1]) == 1  # noqa: S101


class TestRandomName:
    """Тесты для random_name функции."""

    def test_random_name_returns_string(self):
        """Test that random_name returns string."""

        name = random_name()
        assert isinstance(name, str)  # noqa: S101
        assert len(name) > 0  # noqa: S101

    def test_random_name_uniqueness(self):
        """Test that random_name generates unique names."""

        names = {random_name() for _ in range(100)}
        assert len(names) == 100  # noqa: S101

    def test_random_name_no_prefix(self):
        """Test random_name without prefix."""

        name = random_name()
        assert name.count("_") == 1  # noqa: S101

    def test_random_name_different_calls(self):
        """Test multiple calls produce different names."""

        name1 = random_name()
        name2 = random_name()
        assert name1 != name2  # noqa: S101


class TestTransferDiagram:
    """Тесты для transfer_table функции."""

    def test_transfer_table_with_columns(self):
        """Test diagram with columns in both tables."""

        source_columns = OrderedDict(
            [
                ("id", "int4"),
                ("name", "text"),
                ("created_at", "timestamp"),
            ]
        )
        dest_columns = OrderedDict(
            [
                ("id", "Int64"),
                ("name", "String"),
                ("created_at", "Datetime"),
            ]
        )
        source = DBMetadata(
            name="postgres", version="14", columns=source_columns
        )
        dest = DBMetadata(
            name="clickhouse", version="24", columns=dest_columns
        )
        diagram = transfer_table(source, dest)
        assert isinstance(diagram, str)  # noqa: S101
        assert "Transfer data diagram:" in diagram  # noqa: S101
        assert "Source [postgres 14]" in diagram  # noqa: S101
        assert "Destination [clickhouse 24]" in diagram  # noqa: S101
        assert "id" in diagram  # noqa: S101
        assert "name" in diagram  # noqa: S101
        assert "created_at" in diagram  # noqa: S101
        assert "int4" in diagram  # noqa: S101
        assert "String" in diagram  # noqa: S101

    def test_transfer_table_empty_columns(self):
        """Test diagram with empty columns."""

        source = DBMetadata(name="source", version="1", columns=OrderedDict())
        dest = DBMetadata(name="dest", version="1", columns=OrderedDict())
        diagram = transfer_table(source, dest)
        assert isinstance(diagram, str)  # noqa: S101
        assert "Transfer data diagram:" in diagram  # noqa: S101
        assert "Source [source 1]" in diagram  # noqa: S101
        assert "Destination [dest 1]" in diagram  # noqa: S101
        assert "Column Name" in diagram  # noqa: S101
        assert "Data Type" in diagram  # noqa: S101

    def test_transfer_table_single_column(self):
        """Test diagram with single column."""
        source_columns = OrderedDict([("id", "int4")])
        dest_columns = OrderedDict([("id", "Int64")])

        source = DBMetadata(name="source", version="1", columns=source_columns)
        dest = DBMetadata(name="dest", version="1", columns=dest_columns)

        diagram = transfer_table(source, dest)

        assert "id" in diagram  # noqa: S101
        assert "int4" in diagram  # noqa: S101
        assert "Int64" in diagram  # noqa: S101

    def test_transfer_table_long_names(self):
        """Test diagram with long column names."""

        source_columns = OrderedDict(
            [
                ("very_long_column_name_that_exceeds_usual_width", "text"),
                ("another_long_column_name", "int4"),
            ]
        )
        dest_columns = OrderedDict(
            [
                ("very_long_column_name_that_exceeds_usual_width", "String"),
                ("another_long_column_name", "Int64"),
            ]
        )
        source = DBMetadata(name="source", version="1", columns=source_columns)
        dest = DBMetadata(name="dest", version="1", columns=dest_columns)
        diagram = transfer_table(source, dest)
        assert "very_long_column_name_that_exceeds_usual_width" in diagram  # noqa: S101
        assert "another_long_column_name" in diagram  # noqa: S101

    def test_transfer_table_arrow_present(self):
        """Test that diagram contains arrow."""

        source = DBMetadata(name="a", version="1", columns=OrderedDict())
        dest = DBMetadata(name="b", version="1", columns=OrderedDict())
        diagram = transfer_table(source, dest)
        # Проверяем наличие символов стрелки (╲ и ╱)
        assert "╲" in diagram or "╱" in diagram  # noqa: S101


class TestTableDiagram:
    """Тесты для single_table функции."""

    def test_single_table_with_columns(self):

        """Test diagram for single table with columns."""
        columns = OrderedDict(
            [
                ("id", "int4"),
                ("name", "text"),
                ("age", "int2"),
            ]
        )
        metadata = DBMetadata(name="users", version="1", columns=columns)
        diagram = single_table(metadata)
        assert isinstance(diagram, str)  # noqa: S101
        assert "Result table diagram:" in diagram  # noqa: S101
        assert "Summary [users 1]" in diagram  # noqa: S101
        assert "id" in diagram  # noqa: S101
        assert "name" in diagram  # noqa: S101
        assert "age" in diagram  # noqa: S101
        assert "int4" in diagram  # noqa: S101
        assert "text" in diagram  # noqa: S101
        assert "int2" in diagram  # noqa: S101

    def test_single_table_empty_columns(self):
        """Test diagram with empty columns."""

        metadata = DBMetadata(name="empty", version="1", columns=OrderedDict())
        diagram = single_table(metadata)
        assert isinstance(diagram, str)  # noqa: S101
        assert "Result table diagram:" in diagram  # noqa: S101
        assert "Summary [empty 1]" in diagram  # noqa: S101
        assert "Column Name" in diagram  # noqa: S101
        assert "Data Type" in diagram  # noqa: S101

    def test_single_table_single_column(self):
        """Test diagram with single column."""

        columns = OrderedDict([("id", "int4")])
        metadata = DBMetadata(name="single", version="1", columns=columns)
        diagram = single_table(metadata)
        assert "id" in diagram  # noqa: S101
        assert "int4" in diagram  # noqa: S101


class TestLogDiagram:
    """Тесты для log_table функции."""

    def test_log_transfer_table(self, mock_logger):
        """Test logging transfer diagram."""

        source_columns = OrderedDict([("id", "int4")])
        dest_columns = OrderedDict([("id", "Int64")])
        source = DBMetadata(name="source", version="1", columns=source_columns)
        dest = DBMetadata(name="dest", version="1", columns=dest_columns)
        log_table(mock_logger, DumperMode.PROD, source, dest)
        # Проверяем, что logger.info был вызван
        mock_logger.info.assert_called_once()
        call_args = mock_logger.info.call_args[0][0]
        assert "Transfer data diagram:" in call_args  # noqa: S101
        assert "Source [source 1]" in call_args  # noqa: S101
        assert "Destination [dest 1]" in call_args  # noqa: S101

    def test_log_single_table(self, mock_logger):
        """Test logging single table diagram."""

        columns = OrderedDict([("id", "int4")])
        metadata = DBMetadata(name="users", version="1", columns=columns)
        log_table(mock_logger, DumperMode.DEBUG, metadata)
        mock_logger.info.assert_called_once()
        call_args = mock_logger.info.call_args[0][0]
        assert "Result table diagram:" in call_args  # noqa: S101
        assert "Summary [users 1]" in call_args  # noqa: S101

    def test_log_table_test_mode_warning(self, mock_logger):
        """Test warning in TEST mode."""

        columns = OrderedDict([("id", "int4")])
        metadata = DBMetadata(name="users", version="1", columns=columns)
        log_table(mock_logger, DumperMode.TEST, metadata)
        mock_logger.info.assert_called_once()
        mock_logger.warning.assert_called_once()
        warning_msg = mock_logger.warning.call_args[0][0]
        assert "Write functions are not available in TEST mode" in warning_msg  # noqa: S101


if __name__ == "__main__":
    pytest.main([__file__, "-svv"])
