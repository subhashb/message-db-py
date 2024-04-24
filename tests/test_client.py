from unittest.mock import patch

from psycopg2 import OperationalError

from message_db.client import MessageDB
from message_db.connection import ConnectionPool


class TestClientConstruction:
    def test_client_construction_from_url(self):
        store = MessageDB.from_url(
            "postgresql://message_store@localhost:5432/message_store"
        )
        assert store is not None

        assert isinstance(store, MessageDB)
        assert isinstance(store.connection_pool, ConnectionPool)

    def test_client_construction_from_args(self):
        store = MessageDB()
        assert store is not None

        assert isinstance(store, MessageDB)
        assert isinstance(store.connection_pool, ConnectionPool)

    def test_reconnection_after_failure(self, client):
        with patch(
            "psycopg2.connect", side_effect=[OperationalError("Connection lost"), None]
        ):
            try:
                client.read("testStream-123")
            except OperationalError:
                # Retry reading after catching the first failure
                messages = client.read("testStream-123")
                assert messages is not None
