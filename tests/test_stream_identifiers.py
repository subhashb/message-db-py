import pytest


class TestStreamIdentifiers:
    def test_stream_name_raises_error(self, client):
        with pytest.raises(ValueError) as exc:
            client.stream_identifiers("testStream-123")

        assert exc.value.args[0] == "testStream-123 is not a category"

    def test_returns_empty_for_no_messages(self, client):
        identifiers = client.stream_identifiers("testCategory")
        assert identifiers == []

    def test_returns_single_identifier(self, client):
        client.write("testCategory-abc123", "Event1", {"key": "val"})

        identifiers = client.stream_identifiers("testCategory")
        assert identifiers == ["abc123"]

    def test_returns_multiple_unique_identifiers(self, client):
        client.write("testCategory-id1", "Event1", {"k": "v"})
        client.write("testCategory-id2", "Event1", {"k": "v"})
        client.write("testCategory-id3", "Event1", {"k": "v"})

        identifiers = client.stream_identifiers("testCategory")
        assert sorted(identifiers) == ["id1", "id2", "id3"]

    def test_deduplicates_across_events(self, client):
        client.write("testCategory-id1", "Event1", {"k": "v1"})
        client.write("testCategory-id1", "Event2", {"k": "v2"})
        client.write("testCategory-id1", "Event3", {"k": "v3"})

        identifiers = client.stream_identifiers("testCategory")
        assert identifiers == ["id1"]

    def test_excludes_snapshot_streams(self, client):
        client.write("testCategory-id1", "Event1", {"k": "v"})
        client.write("testCategory:snapshot-id1", "SNAPSHOT", {"k": "v"})

        identifiers = client.stream_identifiers("testCategory")
        assert identifiers == ["id1"]

    def test_results_are_sorted(self, client):
        client.write("testCategory-charlie", "Event1", {"k": "v"})
        client.write("testCategory-alpha", "Event1", {"k": "v"})
        client.write("testCategory-bravo", "Event1", {"k": "v"})

        identifiers = client.stream_identifiers("testCategory")
        assert identifiers == ["alpha", "bravo", "charlie"]
