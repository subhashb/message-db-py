import pytest


class TestMessageWrite:
    def test_write_to_store(self, client):
        client.write("testStream-123", "Event1", {"foo": "bar"})

        messages = client.read("testStream-123")
        assert len(messages) == 1

    def test_writing_message_with_metadata(self, client):
        client.write("testStream-123", "Event1", {"foo": "bar"}, {"trace_id": "baz"})

        messages = client.read("testStream-123")
        assert messages[0]["metadata"] == {"trace_id": "baz"}

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

    def test_write_failure_on_no_result_returned_after_cursor_execution(self, client):
        with pytest.raises(ValueError) as exc:
            client.write("testStream-123", "Event1", {"foo": "bar"}, expected_version=1)

        assert (
            exc.value.args[0]
            == "P0001-ERROR:  Wrong expected version: 1 (Stream: testStream-123, Stream Version: -1)"
        )

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
            == "P0001-ERROR:  Wrong expected version: 1 (Stream: testStream-123, Stream Version: 2)"
        )

    def test_concurrent_writes(self, client):
        from threading import Thread

        def write_msg():
            client.write("concurrentStream-123", "Event", {"thread": "value"})

        threads = [Thread(target=write_msg) for _ in range(10)]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join()

        messages = client.read("concurrentStream-123")
        assert len(messages) == 10
