import json

import pytest

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
        client.write("testStream-123", "Event1", {"foo": "bar"})

        messages = client.read("testStream-123", 0, 100)
        assert len(messages) == 1

    def test_writing_message_with_metadata(self, client):
        client.write("testStream-123", "Event1", {"foo": "bar"}, {"trace_id": "baz"})

        messages = client.read("testStream-123", 0, 100)
        assert messages[0]["metadata"] == json.dumps({"trace_id": "baz"})

    def test_that_write_returns_position_of_message_written(self, client):
        position = client.write("testStream-123", "Event1", {"foo": "bar"})
        assert position == 0

        position = client.write("testStream-123", "Event1", {"foo": "bar"})
        assert position == 1

    def test_write_message_batch(self, client):
        events = [
            ("Event1", {"foo1": "bar1"}),
            ("Event2", {"foo2": "bar2"}),
            ("Event3", {"foo3": "bar3"}),
            ("Event4", {"foo4": "bar4"}),
        ]

        last_position = client.write_batch("testStream-123", events)

        assert last_position == 3

    def test_write_with_expected_version(self, client):
        for _ in range(3):
            last_position = client.write("testStream-123", "Event1", {"foo": "bar"})

        try:
            # Position would be at 2 by now, so this should succeed
            client.write(
                "testStream-123",
                "Event1",
                {"foo": "bar"},
                expected_version=last_position,
            )
        except Exception:
            pytest.fail("Unexpected error with expected version")

    def test_that_write_fails_on_expected_version_mismatch(self, client):
        for _ in range(3):
            client.write("testStream-123", "Event1", {"foo": "bar"})
        # Position would be at 2 by now, so this should throw an error
        with pytest.raises(ValueError) as exc:
            client.write("testStream-123", "Event1", {"foo": "bar"}, expected_version=1)

        assert (
            exc.value.args[0]
            == "Wrong expected version: 1 (Stream: testStream-123, Stream Version: 2)"
        )


class TestEventIO:
    def test_read_stream_from_store(self, client):
        client.write("testStream-123", "Event1", {"foo": "bar"})

        messages = client.read("testStream-123", 0, 100)
        assert messages is not None
        assert messages[0]["data"] == json.dumps({"foo": "bar"})

    def test_read_multiple_stream_messages_from_store(self, client):
        for i in range(5):
            client.write("testStream-123", "Event1", {f"foo{i}": f"bar{i}"})

        messages = client.read("testStream-123", 0, 100)

        assert messages is not None
        assert len(messages) == 5
        assert messages[4]["data"] == json.dumps({"foo4": "bar4"})

    def test_read_paginated_stream_messages_from_store(self, client):
        for i in range(5):
            client.write("testStream-123", "Event1", {f"foo{i}": f"bar{i}"})

        messages = client.read("testStream-123", 0, 3)

        assert messages is not None
        assert len(messages) == 3
        assert messages[2]["data"] == json.dumps({"foo2": "bar2"})

    def test_read_stream_last_message(self, client):
        for i in range(5):
            client.write("testStream-123", "Event1", {"foo": f"bar{i}"})

        message = client.read_last_message("testStream-123")
        assert message["position"] == 4
        assert message["data"] == json.dumps({"foo": "bar4"})

    def test_read_specific_stream_message(self, client):
        for i in range(5):
            client.write("testStream-123", "Event1", {"foo": f"bar{i}"})
        for i in range(5):
            client.write("testStream-456", "Event1", {"foo": f"baz{i}"})

        messages = client.read("testStream-456", 0, 100)

        assert len(messages) == 5
        assert messages[4]["data"] == json.dumps({"foo": "baz4"})

    def test_read_category_messages(self, client):
        for i in range(5):
            client.write("testStream-123", "Event1", {"foo": f"bar{i}"})

        messages = client.read("testStream", 0, 100)

        assert len(messages) == 5
        assert messages[4]["data"] == json.dumps({"foo": "bar4"})
