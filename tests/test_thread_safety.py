from unittest.mock import MagicMock

import pytest

from message_db.client import MessageDB
from message_db.connection import ConnectionPool


@pytest.fixture(autouse=True)
def clean_up():
    """Override conftest's autouse clean_up fixture — these tests use mocks only."""
    yield


class TestConnectionReleaseOnError:
    """Verify connections are released back to the pool even when exceptions occur."""

    def _make_mock_client(self):
        pool = MagicMock(spec=ConnectionPool)
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_cursor.execute.side_effect = RuntimeError("query failed")
        mock_conn.cursor.return_value = mock_cursor
        pool.get_connection.return_value = mock_conn
        return MessageDB(connection_pool=pool), pool, mock_conn

    def test_read_releases_connection_on_error(self):
        """read() must release the connection even if cursor.execute raises."""
        client, pool, mock_conn = self._make_mock_client()

        with pytest.raises(RuntimeError, match="query failed"):
            client.read("testStream-123")

        pool.release.assert_called_once_with(mock_conn)

    def test_read_last_message_releases_connection_on_error(self):
        """read_last_message() must release the connection even if cursor.execute raises."""
        client, pool, mock_conn = self._make_mock_client()

        with pytest.raises(RuntimeError, match="query failed"):
            client.read_last_message("testStream-123")

        pool.release.assert_called_once_with(mock_conn)

    def test_stream_identifiers_releases_connection_on_error(self):
        """stream_identifiers() must release the connection even if cursor.execute raises."""
        client, pool, mock_conn = self._make_mock_client()

        with pytest.raises(RuntimeError, match="query failed"):
            client.stream_identifiers("testCategory")

        pool.release.assert_called_once_with(mock_conn)
