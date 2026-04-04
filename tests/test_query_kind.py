import pytest
from base_dumper import get_query_kind


class TestGetQueryKind:
    """Тесты для get_query_kind функции."""

    @pytest.mark.parametrize(
        ("query", "expected"),
        [
            # SELECT запросы
            ("SELECT 1", "Select"),
            ("select 1", "Select"),
            ("SELECT * FROM users", "Select"),
            ("SELECT id, name FROM users WHERE age > 18", "Select"),
            ("WITH cte AS (SELECT 1) SELECT * FROM cte", "Select"),
            # INSERT запросы
            ("INSERT INTO users VALUES (1, 'John')", "Insert"),
            ("insert into users (id, name) values (1, 'John')", "Insert"),
            ("INSERT INTO users SELECT * FROM temp", "Insert"),
            # UPDATE запросы
            ("UPDATE users SET name = 'John' WHERE id = 1", "Update"),
            ("update users set age = 25 where id = 1", "Update"),
            # DELETE запросы
            ("DELETE FROM users WHERE id = 1", "Delete"),
            ("delete from users", "Delete"),
            # CREATE запросы
            ("CREATE TABLE users (id int)", "Create"),
            ("CREATE TEMPORARY TABLE temp (id int)", "Create"),
            ("CREATE INDEX idx_users ON users(id)", "Create"),
            # DROP запросы
            ("DROP TABLE users", "Drop"),
            ("DROP INDEX IF EXISTS idx_users", "Drop"),
            # ALTER запросы
            ("ALTER TABLE users ADD COLUMN age int", "Alter"),
            # ANALYZE запросы
            ("ANALYZE users", "Analyze"),
            ("ANALYZE default.table", "Analyze"),
            ("analyze users", "Analyze"),
            # Другие команды
            ("VACUUM users", "Vacuum"),
            ("REINDEX TABLE users", "Reindex"),
            ("GRANT SELECT ON users TO user", "Grant"),
            ("COMMIT", "Commit"),
            ("BEGIN", "Begin"),
            # Пустой запрос
            ("", "Unknown"),
            ("   ", "Unknown"),
            # Комментарии
            ("-- comment\nSELECT 1", "Select"),
            ("/* comment */ SELECT 1", "Select"),
            # Сложные запросы
            ("SELECT * FROM users; SELECT * FROM orders", "Select"),
            ("INSERT INTO users (id) VALUES (1); SELECT 1", "Insert"),
        ],
    )
    def test_get_query_kind(self, query: str, expected: str):
        """Test query kind detection."""

        result = get_query_kind(query)
        assert result == expected, f"Query: {query!r}"  # noqa: S101

    def test_get_query_kind_with_invalid_sql(self):
        """Test with invalid SQL."""

        result = get_query_kind("INVALID SQL !!!")
        assert result == "Invalid"  # noqa: S101

    def test_get_query_kind_with_multiple_statements(self):
        """Test with multiple statements."""

        query = "SELECT 1; SELECT 2; SELECT 3"
        result = get_query_kind(query)
        assert result == "Select"  # noqa: S101

    def test_get_query_kind_with_cte(self):
        """Test with CTE (WITH clause)."""

        query = "WITH t AS (SELECT 1) SELECT * FROM t"
        result = get_query_kind(query)
        assert result == "Select"  # noqa: S101

    def test_get_query_kind_with_subquery(self):
        """Test with subquery."""

        query = "SELECT * FROM (SELECT 1) t"
        result = get_query_kind(query)
        assert result == "Select"  # noqa: S101


if __name__ == "__main__":
    pytest.main([__file__, "-svv"])
