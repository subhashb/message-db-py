import pytest


class TestReadCategory:
    def test_reading_a_stream_throws_error(self, client):
        with pytest.raises(ValueError) as exc:
            client.read_category("testStream-123")

        assert exc.value.args[0] == "testStream-123 is not a category"

    def test_read_category_messages_from_store(self, client):
        client.write("testStream-123", "Event1", {"foo": "bar"})

        messages = client.read_category("testStream")
        assert messages is not None
        assert messages[0]["data"] == {"foo": "bar"}

    def test_read_multiple_category_messages_from_store(self, client):
        for i in range(5):
            client.write("testStream-123", "Event1", {f"foo{i}": f"bar{i}"})

        messages = client.read_category("testStream")

        assert messages is not None
        assert len(messages) == 5
        assert messages[4]["data"] == {"foo4": "bar4"}

    def test_read_paginated_category_messages_from_store(self, client):
        for i in range(5):
            client.write("testStream-123", "Event1", {f"foo{i}": f"bar{i}"})

        messages = client.read_category("testStream", no_of_messages=3)

        assert messages is not None
        assert len(messages) == 3
        assert messages[2]["data"] == {"foo2": "bar2"}
