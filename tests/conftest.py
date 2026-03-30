import pytest
from unittest.mock import MagicMock
from base_dumper import DBConnector


@pytest.fixture
def sample_connector():
    """Create sample DBConnector for tests."""
    return DBConnector(
        host="localhost",
        dbname="testdb",
        user="testuser",
        password="testpass",  # noqa: S106
        port=5432,
    )


@pytest.fixture
def sample_fileobj():
    """Create mock file object."""
    fileobj = MagicMock()
    fileobj.write = MagicMock(return_value=0)
    fileobj.read = MagicMock(return_value=b"")
    fileobj.tell = MagicMock(return_value=0)
    return fileobj


@pytest.fixture
def mock_logger():
    """Create mock logger."""
    logger = MagicMock()
    logger.info = MagicMock()
    logger.error = MagicMock()
    logger.debug = MagicMock()
    logger.warning = MagicMock()
    return logger
