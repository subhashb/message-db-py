from __future__ import annotations

from typing import Dict
from uuid import uuid4

from psycopg2.extensions import connection
from psycopg2.extras import Json, RealDictCursor

from message_db.connection import ConnectionPool


class MessageDB:
    """This class provides a Python interface to all MessageDB commands."""

    @classmethod
    def from_url(cls, url: str, **kwargs: str) -> MessageDB:
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
        password: str = None,
        host: str = "localhost",
        port: int = 5432,
        connection_pool: ConnectionPool = None,
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
        data: Dict,
        metadata: Dict = None,
        expected_version: int = None,
    ):
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
        except Exception as exc:
            raise ValueError(exc.args[0].splitlines()[0]) from exc

        return result["write_message"]

    def write(
        self,
        stream_name: str,
        message_type: str,
        data: Dict,
        metadata: Dict = None,
        expected_version: int = None,
    ):
        conn = self.connection_pool.get_connection()

        try:
            with conn:
                position = self._write(
                    conn, stream_name, message_type, data, metadata, expected_version
                )
        finally:
            self.connection_pool.release(conn)

        return position

    def write_batch(self, stream_name, data, expected_version: int = None):
        conn = self.connection_pool.get_connection()

        try:
            with conn:
                for record in data:
                    expected_version = self._write(
                        conn,
                        stream_name,
                        record[0],
                        record[1],
                        metadata=record[2] if len(record) > 2 else None,
                        expected_version=expected_version,
                    )
        finally:
            self.connection_pool.release(conn)

        return expected_version

    def read(self, stream_name, position=0, no_of_messages=1000):
        conn = self.connection_pool.get_connection()
        cursor = conn.cursor(cursor_factory=RealDictCursor)

        if "-" in stream_name:
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
        messages = cursor.fetchall()

        conn.commit()
        cursor.close()
        self.connection_pool.release(conn)

        return messages

    def read_last_message(self, stream_name):
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

        return message
