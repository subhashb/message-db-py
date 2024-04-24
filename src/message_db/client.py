from __future__ import annotations

import json
from typing import Any, Dict, List
from uuid import uuid4

from psycopg2 import DatabaseError
from psycopg2.extensions import connection
from psycopg2.extras import Json, RealDictCursor

from message_db.connection import ConnectionPool


class MessageDB:
    """This class provides a Python interface to all MessageDB commands."""

    @classmethod
    def from_url(cls, url: str, **kwargs: Any) -> MessageDB:
        """Returns a MessageDB client object configured from the given URL.

        The general form of a connection string is:
            postgresql://[user[:password]@][netloc][:port][/database_name][?param1=value1&...]

        Examples:
            postgresql://john:not-so-secret@localhost:5432/test_db

        Args:
            url (str): Postgres-compliant URL connection string

        Returns:
            MessageDB: MessageDB client object
        """
        connection_pool = ConnectionPool.from_url(url, **kwargs)
        return cls(connection_pool=connection_pool)

    def __init__(
        self,
        dbname: str = "message_store",
        user: str = "message_store",
        password: str = "",
        host: str = "localhost",
        port: int = 5432,
        connection_pool: ConnectionPool | None = None,
    ) -> None:
        if not connection_pool:
            connection_pool = ConnectionPool(
                dbname=dbname, user=user, password=password, host=host, port=port
            )
        self.connection_pool = connection_pool

    def _write(
        self,
        connection: connection,
        stream_name: str,
        message_type: str,
        data: Dict[str, Any],
        metadata: Dict[str, Any] | None = None,
        expected_version: int | None = None,
    ) -> int:
        """Write a message to a stream."""
        try:
            with connection.cursor(cursor_factory=RealDictCursor) as cursor:
                cursor.execute(
                    (
                        "SELECT message_store.write_message(%(identifier)s, %(stream_name)s, %(type)s, "
                        "%(data)s, %(metadata)s, %(expected_version)s);"
                    ),
                    {
                        "identifier": str(uuid4()),
                        "stream_name": stream_name,
                        "type": message_type,
                        "data": Json(data),
                        "metadata": Json(metadata) if metadata else None,
                        "expected_version": expected_version,
                    },
                )

                result = cursor.fetchone()
                if result is None:
                    raise ValueError("No result returned from the database operation.")
        except DatabaseError as exc:
            raise ValueError(
                f"{getattr(exc, 'pgcode')}-{getattr(exc, 'pgerror').splitlines()[0]}"
            ) from exc

        return result["write_message"]

    def write(
        self,
        stream_name: str,
        message_type: str,
        data: Dict,
        metadata: Dict | None = None,
        expected_version: int | None = None,
    ) -> int:
        """Write a message to a stream."""
        conn = self.connection_pool.get_connection()

        try:
            with conn:
                position = self._write(
                    conn, stream_name, message_type, data, metadata, expected_version
                )
        finally:
            self.connection_pool.release(conn)

        return position

    def write_batch(
        self, stream_name, data, expected_version: int | None = None
    ) -> int:
        """Write a batch of messages to a stream."""
        conn = self.connection_pool.get_connection()

        try:
            with conn:
                for record in data:
                    position = self._write(
                        conn,
                        stream_name,
                        record[0],
                        record[1],
                        metadata=record[2] if len(record) > 2 else None,
                        expected_version=expected_version,
                    )

                    expected_version = position
        finally:
            self.connection_pool.release(conn)

        return position

    def read(
        self,
        stream_name: str,
        sql: str | None = None,
        position: int = 0,
        no_of_messages: int = 1000,
    ) -> List[Dict[str, Any]]:
        """Read messages from a stream or category.

        Returns a list of messages from the stream or category starting from the given position.
        """
        conn = self.connection_pool.get_connection()
        cursor = conn.cursor(cursor_factory=RealDictCursor)

        if not sql:
            if stream_name == "$all":
                sql = """
                    SELECT
                        id::varchar,
                        stream_name::varchar,
                        type::varchar,
                        position::bigint,
                        global_position::bigint,
                        data::varchar,
                        metadata::varchar,
                        time::timestamp
                    FROM
                        messages
                    WHERE
                        global_position > %(position)s
                    LIMIT %(batch_size)s
                """
            elif "-" in stream_name:
                sql = "SELECT * FROM get_stream_messages(%(stream_name)s, %(position)s, %(batch_size)s);"
            else:
                sql = "SELECT * FROM get_category_messages(%(stream_name)s, %(position)s, %(batch_size)s);"

        cursor.execute(
            sql,
            {
                "stream_name": stream_name,
                "position": position,
                "batch_size": no_of_messages,
            },
        )
        raw_messages = cursor.fetchall()

        conn.commit()
        cursor.close()
        self.connection_pool.release(conn)

        messages = []
        for message in raw_messages:
            processed_message = dict(
                message
            )  # Convert each RealDictRow to a dictionary
            processed_message["data"] = json.loads(processed_message["data"])
            processed_message["metadata"] = (
                json.loads(processed_message["metadata"])
                if processed_message["metadata"]
                else None
            )
            messages.append(processed_message)

        return messages

    def read_stream(
        self, stream_name: str, position: int = 0, no_of_messages: int = 1000
    ) -> List[Dict[str, Any]]:
        """Read messages from a stream.

        Returns a list of messages from the stream starting from the given position.
        """
        if "-" not in stream_name:
            raise ValueError(f"{stream_name} is not a stream")

        sql = "SELECT * FROM get_stream_messages(%(stream_name)s, %(position)s, %(batch_size)s);"

        return self.read(
            stream_name, sql=sql, position=position, no_of_messages=no_of_messages
        )

    def read_category(
        self, category_name: str, position: int = 0, no_of_messages: int = 1000
    ) -> List[Dict[str, Any]]:
        """Read messages from a category.

        Returns a list of messages from the category starting from the given position.
        """
        if "-" in category_name:
            raise ValueError(f"{category_name} is not a category")

        sql = "SELECT * FROM get_category_messages(%(stream_name)s, %(position)s, %(batch_size)s);"

        return self.read(
            category_name, sql=sql, position=position, no_of_messages=no_of_messages
        )

    def read_last_message(self, stream_name: str) -> Dict[str, Any] | None:
        """Read the last message from a stream."""
        conn = self.connection_pool.get_connection()
        cursor = conn.cursor(cursor_factory=RealDictCursor)

        cursor.execute(
            "SELECT * from get_last_stream_message(%(stream_name)s);",
            {"stream_name": stream_name},
        )

        message = cursor.fetchone()

        conn.commit()
        cursor.close()
        self.connection_pool.release(conn)

        if message:
            message["data"] = json.loads(message["data"])
            message["metadata"] = (
                json.loads(message["metadata"]) if message["metadata"] else None
            )
        return message
