import pytest
from base_dumper import DBConnector


class TestDBConnector:
    """Тесты для DBConnector dataclass."""

    def test_creation_full(self, sample_connector: DBConnector):
        """Test DBConnector creation with all fields."""

        assert sample_connector.host == "localhost"  # noqa: S101
        assert sample_connector.dbname == "testdb"  # noqa: S101
        assert sample_connector.user == "testuser"  # noqa: S101
        assert sample_connector.password == "testpass"  # noqa: S101, S105
        assert sample_connector.port == 5432  # noqa: S101

    def test_immutability(self, sample_connector: DBConnector):
        """Test that DBConnector is immutable (frozen)."""

        with pytest.raises(AttributeError):
            sample_connector.host = "otherhost"  # type: ignore

    def test_equality(self):
        """Test DBConnector equality."""

        conn1 = DBConnector("localhost", "testdb", "nobody", "", 5432)
        conn2 = DBConnector(
            host="localhost",
            dbname="testdb",
            user="nobody",
            password="",
            port=5432,
        )
        conn3 = DBConnector(
            host="otherhost",
            dbname="testdb",
            user="otheruser",
            password="",
            port=5432,
        )

        assert conn1 == conn2  # noqa: S101
        assert conn1 != conn3  # noqa: S101

    def test_repr(self, sample_connector):
        """Test DBConnector string representation."""

        repr_str = repr(sample_connector)
        assert "DBConnector" in repr_str  # noqa: S101
        assert "localhost" in repr_str  # noqa: S101
        assert "testdb" in repr_str  # noqa: S101


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
