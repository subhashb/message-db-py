import psycopg2


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

    def test_reading_all_streams_in_global_position_order_after_an_update(self, client):
        for i in range(3):
            client.write("stream1-123", "Event1", {"foo": f"bar{i}"})

        # An UPDATE writes a new row version at the end of the table, so physical
        # row order no longer matches global_position
        conn = psycopg2.connect(
            dbname="message_store", user="postgres", port=5432, host="localhost"
        )
        cursor = conn.cursor()
        cursor.execute(
            "UPDATE message_store.messages SET metadata = '{}' WHERE global_position = 1;"
        )
        conn.commit()
        cursor.close()
        conn.close()

        messages = client.read("$all")
        assert [m["global_position"] for m in messages] == [1, 2, 3]

        messages = client.read("$all", no_of_messages=2)
        assert [m["global_position"] for m in messages] == [1, 2]
