import pytest


class TestConsumerGroupValidation:
    def test_consumer_group_member_without_size_raises_error(self, client):
        with pytest.raises(ValueError) as exc:
            client.read_category("testStream", consumer_group_member=0)

        assert (
            "Both consumer_group_member and consumer_group_size must be provided together"
            in str(exc.value)
        )

    def test_consumer_group_size_without_member_raises_error(self, client):
        with pytest.raises(ValueError) as exc:
            client.read_category("testStream", consumer_group_size=3)

        assert (
            "Both consumer_group_member and consumer_group_size must be provided together"
            in str(exc.value)
        )

    def test_negative_consumer_group_member_raises_error(self, client):
        with pytest.raises(ValueError) as exc:
            client.read_category(
                "testStream", consumer_group_member=-1, consumer_group_size=3
            )

        assert "consumer_group_member must be >= 0" in str(exc.value)

    def test_zero_consumer_group_size_raises_error(self, client):
        with pytest.raises(ValueError) as exc:
            client.read_category(
                "testStream", consumer_group_member=0, consumer_group_size=0
            )

        assert "consumer_group_size must be > 0" in str(exc.value)

    def test_negative_consumer_group_size_raises_error(self, client):
        with pytest.raises(ValueError) as exc:
            client.read_category(
                "testStream", consumer_group_member=0, consumer_group_size=-1
            )

        assert "consumer_group_size must be > 0" in str(exc.value)

    def test_consumer_group_member_exceeds_size_raises_error(self, client):
        with pytest.raises(ValueError) as exc:
            client.read_category(
                "testStream", consumer_group_member=3, consumer_group_size=3
            )

        assert (
            "consumer_group_member (3) must be less than consumer_group_size (3)"
            in str(exc.value)
        )

    def test_consumer_group_member_equal_to_size_minus_one_is_valid(self, client):
        # Should not raise an error
        messages = client.read_category(
            "testStream", consumer_group_member=2, consumer_group_size=3
        )
        assert messages is not None


class TestConsumerGroupFunctionality:
    def test_single_consumer_group_member_receives_all_messages(self, client):
        # Write messages to multiple streams in the same category
        for i in range(5):
            client.write(f"testStream-{i}", "Event1", {"index": i})

        messages = client.read_category(
            "testStream", consumer_group_member=0, consumer_group_size=1
        )

        assert len(messages) == 5

    def test_consumer_group_members_receive_non_overlapping_streams(self, client):
        # Write messages to multiple streams in the same category
        stream_ids = [f"stream-{i}" for i in range(10)]
        for stream_id in stream_ids:
            client.write(f"testStream-{stream_id}", "Event1", {"stream": stream_id})

        # Read with 3 consumer group members
        messages_0 = client.read_category(
            "testStream", consumer_group_member=0, consumer_group_size=3
        )
        messages_1 = client.read_category(
            "testStream", consumer_group_member=1, consumer_group_size=3
        )
        messages_2 = client.read_category(
            "testStream", consumer_group_member=2, consumer_group_size=3
        )

        # Extract stream names from each consumer's messages
        streams_0 = {msg["stream_name"] for msg in messages_0}
        streams_1 = {msg["stream_name"] for msg in messages_1}
        streams_2 = {msg["stream_name"] for msg in messages_2}

        # Verify no overlap
        assert streams_0.isdisjoint(streams_1)
        assert streams_0.isdisjoint(streams_2)
        assert streams_1.isdisjoint(streams_2)

    def test_all_streams_distributed_among_consumer_group_members(self, client):
        # Write messages to multiple streams in the same category
        stream_ids = [f"stream-{i}" for i in range(10)]
        for stream_id in stream_ids:
            client.write(f"testStream-{stream_id}", "Event1", {"stream": stream_id})

        # Read with 3 consumer group members
        messages_0 = client.read_category(
            "testStream", consumer_group_member=0, consumer_group_size=3
        )
        messages_1 = client.read_category(
            "testStream", consumer_group_member=1, consumer_group_size=3
        )
        messages_2 = client.read_category(
            "testStream", consumer_group_member=2, consumer_group_size=3
        )

        # All messages should be distributed
        total_messages = len(messages_0) + len(messages_1) + len(messages_2)
        assert total_messages == 10

        # Verify all streams were covered
        all_streams = {
            msg["stream_name"] for msg in messages_0 + messages_1 + messages_2
        }
        expected_streams = {f"testStream-stream-{i}" for i in range(10)}
        assert all_streams == expected_streams

    def test_same_stream_consistently_assigned_to_same_consumer(self, client):
        # Write messages to the same stream multiple times
        for i in range(5):
            client.write("testStream-123", "Event1", {"index": i})

        # Read with consumer group member 0
        messages_0_first = client.read_category(
            "testStream", consumer_group_member=0, consumer_group_size=3
        )
        messages_1_first = client.read_category(
            "testStream", consumer_group_member=1, consumer_group_size=3
        )

        # Write more messages to the same stream
        for i in range(5, 10):
            client.write("testStream-123", "Event1", {"index": i})

        # Read again with the same consumer group configuration
        messages_0_second = client.read_category(
            "testStream", consumer_group_member=0, consumer_group_size=3
        )
        messages_1_second = client.read_category(
            "testStream", consumer_group_member=1, consumer_group_size=3
        )

        # The same consumer should get all messages from the stream
        # Either consumer 0 gets all or consumer 1 gets all, but not both
        if len(messages_0_first) > 0:
            assert len(messages_1_first) == 0
            assert len(messages_0_second) == 10
            assert len(messages_1_second) == 0
        else:
            assert len(messages_1_first) == 5
            assert len(messages_0_second) == 0
            assert len(messages_1_second) == 10

    def test_consumer_group_with_pagination(self, client):
        # Write multiple messages to multiple streams
        for i in range(5):
            for j in range(3):
                client.write(f"testStream-{i}", "Event1", {"stream": i, "index": j})

        # Read with pagination
        messages_page1 = client.read_category(
            "testStream",
            no_of_messages=5,
            consumer_group_member=0,
            consumer_group_size=2,
        )

        # Get the last global position from the first page
        if messages_page1:
            last_position = messages_page1[-1]["global_position"]
            messages_page2 = client.read_category(
                "testStream",
                position=last_position,
                no_of_messages=5,
                consumer_group_member=0,
                consumer_group_size=2,
            )

            # Should have messages in both pages if this consumer got multiple streams
            # The exact count depends on which streams hash to this consumer
            assert messages_page1 is not None
            assert isinstance(messages_page2, list)

    def test_consumer_group_with_empty_category(self, client):
        # Read from non-existent category with consumer groups
        messages = client.read_category(
            "emptyCategory", consumer_group_member=0, consumer_group_size=3
        )

        assert messages == []

    def test_consumer_group_preserves_message_order_within_stream(self, client):
        # Write multiple messages to a single stream
        for i in range(10):
            client.write("testStream-abc", "Event1", {"index": i})

        # Read with consumer group
        messages = client.read_category(
            "testStream", consumer_group_member=0, consumer_group_size=3
        )

        # If this consumer got the stream, verify order
        if len(messages) > 0:
            assert len(messages) == 10
            for i in range(10):
                assert messages[i]["data"]["index"] == i
                assert messages[i]["position"] == i


class TestConsumerGroupEdgeCases:
    def test_consumer_group_size_one(self, client):
        # Single consumer group should get all messages
        for i in range(5):
            client.write(f"testStream-{i}", "Event1", {"index": i})

        messages = client.read_category(
            "testStream", consumer_group_member=0, consumer_group_size=1
        )

        assert len(messages) == 5

    def test_large_consumer_group_size(self, client):
        # Create fewer streams than group size
        for i in range(3):
            client.write(f"testStream-{i}", "Event1", {"index": i})

        # Some consumers will get no messages
        all_messages = []
        for member in range(10):
            messages = client.read_category(
                "testStream", consumer_group_member=member, consumer_group_size=10
            )
            all_messages.extend(messages)

        # All messages should still be retrieved across all consumers
        assert len(all_messages) == 3

    def test_consumer_group_with_multiple_messages_per_stream(self, client):
        # Write multiple messages to each stream
        for stream_id in range(3):
            for msg_idx in range(5):
                client.write(
                    f"testStream-{stream_id}",
                    "Event1",
                    {"stream": stream_id, "message": msg_idx},
                )

        # Each consumer should get all messages from their assigned streams
        messages_0 = client.read_category(
            "testStream", consumer_group_member=0, consumer_group_size=2
        )
        messages_1 = client.read_category(
            "testStream", consumer_group_member=1, consumer_group_size=2
        )

        # Total should be 15 (3 streams * 5 messages)
        assert len(messages_0) + len(messages_1) == 15

        # Each consumer should get complete streams (5 messages each)
        # The actual distribution depends on hashing
        for messages in [messages_0, messages_1]:
            if len(messages) > 0:
                # Group by stream and verify each stream has 5 messages
                streams = {}
                for msg in messages:
                    stream_name = msg["stream_name"]
                    streams[stream_name] = streams.get(stream_name, 0) + 1

                for count in streams.values():
                    assert count == 5
