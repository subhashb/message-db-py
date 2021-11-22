from __future__ import annotations

from typing import Any

from psycopg2.extensions import connection
from psycopg2.pool import SimpleConnectionPool


class ConnectionPool:
    @classmethod
    def from_url(
        cls, *args: str, max_connections: int = 100, **kwargs: Any
    ) -> ConnectionPool:
        """Return a Connection Pool configured from the given URL.

        Args:
            max_connections (int): Maximum no. of connections
            args (str): Arguments to pass to psycopg2 `connect()`
            kwargs (str): Keyword arguments to pass to psycopg2 `connect()`

        Returns:
            ConnectionPool: A configured connection pool object
        """
        return cls(*args, max_connections=max_connections, **kwargs)

    def __init__(self, *args: str, max_connections: int = 100, **kwargs: Any) -> None:
        if not isinstance(max_connections, int) or max_connections < 0:
            raise ValueError('"max_connections" must be a positive integer')

        self.max_connections = max_connections
        self.args = args
        self.kwargs = kwargs

        # Minimum connections start at 1
        min_connections = 1

        self._connection_pool = SimpleConnectionPool(
            min_connections, self.max_connections, *self.args, **self.kwargs
        )

    def get_connection(self) -> connection:
        """Retrieve a connection from the connection pool

        Returns:
            connection: the connection to a PostgreSQL database instance.
        """
        return self._connection_pool.getconn()

    def release(self, connection: connection, close: bool = False) -> None:
        """Release a connection back into the pool"""
        self._connection_pool.putconn(connection, close=close)

    def closeall(self) -> None:
        """Close all connections handled by the pool."""
        self._connection_pool.closeall()
