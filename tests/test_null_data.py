import psycopg2


def _set_data_to_null(stream_name, position):
    # Message DB's write_message always stores data, so a NULL row has to be made directly
    conn = psycopg2.connect(
        dbname="message_store", user="postgres", port=5432, host="localhost"
    )
    cursor = conn.cursor()
    cursor.execute(
        "UPDATE message_store.messages SET data = NULL "
        "WHERE stream_name = %(stream_name)s AND position = %(position)s;",
        {"stream_name": stream_name, "position": position},
    )
    conn.commit()
    cursor.close()
    conn.close()


class TestNullData:
    def test_read_returns_null_data_as_none(self, client):
        for i in range(3):
            client.write("test-abc", "Event1", {"foo": f"bar{i}"})
        _set_data_to_null("test-abc", 1)

        messages = client.read("test-abc")

        assert len(messages) == 3
        assert messages[1]["data"] is None
        assert messages[0]["data"] == {"foo": "bar0"}
        assert messages[2]["data"] == {"foo": "bar2"}

    def test_read_all_returns_null_data_as_none(self, client):
        for i in range(3):
            client.write("test-abc", "Event1", {"foo": f"bar{i}"})
        _set_data_to_null("test-abc", 1)

        messages = client.read("$all")

        assert len(messages) == 3
        assert messages[1]["data"] is None
        assert messages[2]["data"] == {"foo": "bar2"}

    def test_read_last_message_returns_null_data_as_none(self, client):
        for i in range(3):
            client.write("test-abc", "Event1", {"foo": f"bar{i}"})
        _set_data_to_null("test-abc", 2)

        message = client.read_last_message("test-abc")

        assert message["position"] == 2
        assert message["data"] is None

    def test_read_last_message_parses_data_when_an_earlier_row_is_null(self, client):
        for i in range(3):
            client.write("test-abc", "Event1", {"foo": f"bar{i}"})
        _set_data_to_null("test-abc", 1)

        message = client.read_last_message("test-abc")

        assert message["data"] == {"foo": "bar2"}
