import pytest


class TestReadStream:
    def test_reading_a_category_throws_error(self, client):
        with pytest.raises(ValueError) as exc:
            client.read_stream("testStream")

        assert exc.value.args[0] == "testStream is not a stream"

    def test_read_stream_from_store(self, client):
        client.write("testStream-123", "Event1", {"foo": "bar"})

        messages = client.read_stream("testStream-123")
        assert messages is not None
        assert messages[0]["data"] == {"foo": "bar"}

    def test_read_multiple_stream_messages_from_store(self, client):
        for i in range(5):
            client.write("testStream-123", "Event1", {f"foo{i}": f"bar{i}"})

        messages = client.read_stream("testStream-123")

        assert messages is not None
        assert len(messages) == 5
        assert messages[4]["data"] == {"foo4": "bar4"}

    def test_read_paginated_stream_messages_from_store(self, client):
        for i in range(5):
            client.write("testStream-123", "Event1", {f"foo{i}": f"bar{i}"})

        messages = client.read_stream("testStream-123", no_of_messages=3)

        assert messages is not None
        assert len(messages) == 3
        assert messages[2]["data"] == {"foo2": "bar2"}
