class TestReadAll:
    def test_reading_all_streams(self, client):
        client.write("stream1-123", "Event1", {"foo": "bar"})
        client.write("stream2-123", "Event2", {"foo": "bar"})

        messages = client.read("$all")
        assert len(messages) == 2

    def test_reading_all_streams_after_position(self, client):
        for i in range(5):
            client.write("stream1-123", f"Event1{i+1}", {"foo": "bar"})
        for i in range(5):
            client.write("stream2-123", f"Event2{i+1}", {"foo": "bar"})

        messages = client.read("$all")
        assert len(messages) == 10
        messages = client.read("$all", position=5)
        assert len(messages) == 5
        messages = client.read("$all", position=5, no_of_messages=2)
        assert len(messages) == 2
