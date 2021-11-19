from __future__ import annotations

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
        dbname: str = "postgres",
        user: str = "postgres",
        password: str = None,
        host: str = "localhost",
        port: int = 5432,
        connection_pool: ConnectionPool = None,
    ) -> None:
        if not connection_pool:
            connection_pool = ConnectionPool(
                dbname=dbname,
                user=user,
                password=password,
                host=host,
                port=port
            )
        self.connection_pool = connection_pool
