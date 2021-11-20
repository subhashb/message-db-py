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


class TestMessageWrite:
    def test_write_to_store(self, client):
        pass

    def test_writing_message_with_metadata(self, client):
        pass

    def test_that_write_returns_position_of_message_written(self, client):
        position = client.write("testStream-123", "Event1", {"foo": "bar"})
        assert position == 0

        position = client.write("testStream-123", "Event1", {"foo": "bar"})
        assert position == 1

    def test_that_global_position_is_incremented_on_write(self, client):
        pass

    def test_write_message_batch(self, client):
        pass

    def test_write_with_expected_version(self, client):
        pass

    def test_that_write_fails_on_expected_version_mismatch(self, client):
        pass


class TestEventIO:
    def test_read_stream_from_store(self, client):
        pass

    def test_read_multiple_stream_messages_from_store(self, client):
        pass

    def test_read_stream_last_message(self, client):
        pass

    def test_read_specific_stream_message(self, client):
        pass

    def test_read_category_messages(self, client):
        pass
