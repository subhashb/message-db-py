from __future__ import annotations

from typing import Dict
from uuid import uuid4

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

    def write(
        self,
        stream_name: str,
        message_type: str,
        data: Dict,
        metadata: Dict = None,
        expected_version: int = None,
    ):
        conn = self.connection_pool.get_connection()
        cursor = conn.cursor(cursor_factory=RealDictCursor)

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
        conn.commit()

        result = cursor.fetchone()

        self.connection_pool.release(conn)
        return result["write_message"]
