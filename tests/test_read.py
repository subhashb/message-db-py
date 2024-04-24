class TestRead:
    def test_read_stream_from_store(self, client):
        client.write("testStream-123", "Event1", {"foo": "bar"})

        messages = client.read("testStream-123")
        assert messages is not None
        assert messages[0]["data"] == {"foo": "bar"}

    def test_read_multiple_stream_messages_from_store(self, client):
        for i in range(5):
            client.write("testStream-123", "Event1", {f"foo{i}": f"bar{i}"})

        messages = client.read("testStream-123")

        assert messages is not None
        assert len(messages) == 5
        assert messages[4]["data"] == {"foo4": "bar4"}

    def test_read_paginated_stream_messages_from_store(self, client):
        for i in range(5):
            client.write("testStream-123", "Event1", {f"foo{i}": f"bar{i}"})

        messages = client.read("testStream-123", no_of_messages=3)

        assert messages is not None
        assert len(messages) == 3
        assert messages[2]["data"] == {"foo2": "bar2"}

    def test_read_stream_last_message(self, client):
        for i in range(5):
            client.write("testStream-123", "Event1", {"foo": f"bar{i}"})

        message = client.read_last_message("testStream-123")
        assert message["position"] == 4
        assert message["data"] == {"foo": "bar4"}

    def test_last_message_empty_stream(self, client):
        result = client.read_last_message("emptyStream-123")
        assert result is None

    def test_read_from_non_existing_stream(self, client):
        messages = client.read("nonExistingStream-123")
        assert messages == []

    def test_read_specific_stream_message(self, client):
        for i in range(5):
            client.write("testStream-123", "Event1", {"foo": f"bar{i}"})
        for i in range(5):
            client.write("testStream-456", "Event1", {"foo": f"baz{i}"})

        messages = client.read("testStream-456")

        assert len(messages) == 5
        assert messages[4]["data"] == {"foo": "baz4"}

    def test_read_category_messages(self, client):
        for i in range(5):
            client.write("testStream-123", "Event1", {"foo": f"bar{i}"})

        messages = client.read("testStream")

        assert len(messages) == 5
        assert messages[4]["data"] == {"foo": "bar4"}
