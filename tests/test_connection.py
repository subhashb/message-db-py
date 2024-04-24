from unittest.mock import patch

import pytest
from psycopg2 import OperationalError, ProgrammingError
from psycopg2.extensions import TRANSACTION_STATUS_ACTIVE
from psycopg2.pool import PoolError

from message_db.connection import ConnectionPool

CONNECT_URL = "postgresql://message_store@localhost:5432/message_store"


def test_constructing_connection_pool_from_url(pool):
    assert pool is not None
    assert isinstance(pool, ConnectionPool)


def test_error_on_invalid_url():
    with pytest.raises(ProgrammingError) as exc:
        ConnectionPool("foo://bar")

    assert exc.value.args[0].startswith("invalid dsn")


def test_error_on_invalid_role():
    with pytest.raises(OperationalError) as exc:
        ConnectionPool("postgresql://foo@localhost:5432/message_store")

    assert 'role "foo" does not exist' in exc.value.args[0]


def test_error_on_invalid_max_connections():
    with pytest.raises(ValueError):
        ConnectionPool(CONNECT_URL, max_connections=-1)


def test_retrieving_connection_from_pool(pool):
    used_count = len(pool._connection_pool._used)

    conn = pool.get_connection()
    assert conn is not None
    assert conn.status == TRANSACTION_STATUS_ACTIVE

    assert len(pool._connection_pool._used) == used_count + 1


def test_releasing_a_connection(pool):
    conn = pool.get_connection()

    used_count = len(pool._connection_pool._used)

    pool.release(conn)
    assert len(pool._connection_pool._used) == used_count - 1


def test_connection_pool_initialization():
    """Test the connection pool initialization with different parameters."""
    pool = ConnectionPool(CONNECT_URL, max_connections=10)
    assert pool.max_connections == 10
    assert pool._connection_pool.maxconn == 10


def test_multiple_simultaneous_connections():
    """Test handling multiple simultaneous connections within max limit."""
    pool = ConnectionPool(CONNECT_URL, max_connections=5)
    connections = [pool.get_connection() for _ in range(5)]
    assert len(pool._connection_pool._used) == 5
    # Clean up
    for conn in connections:
        pool.release(conn)


def test_exceeding_connection_limit():
    """Test behavior when more connections than max are requested."""
    pool = ConnectionPool(CONNECT_URL, max_connections=2)
    conn1 = pool.get_connection()
    conn2 = pool.get_connection()
    with pytest.raises(Exception) as exc:
        pool.get_connection()
    assert "connection pool exhausted" in str(exc.value)
    # Clean up
    pool.release(conn1)
    pool.release(conn2)


def test_release_invalid_connection():
    """Test releasing a connection that was never retrieved."""
    pool = ConnectionPool(CONNECT_URL, max_connections=5)
    fake_conn = None  # Simulating an invalid connection
    with pytest.raises(PoolError) as exc:
        pool.release(fake_conn)
    assert "trying to put unkeyed connection" in str(exc.value)


def test_close_all_connections():
    """Test closing all connections."""
    pool = ConnectionPool(CONNECT_URL, max_connections=5)
    connections = [pool.get_connection() for _ in range(5)]
    pool.closeall()
    assert all(conn.closed for conn in connections)


def test_network_error_on_connection():
    """Test handling network errors during connection retrieval."""
    with patch("psycopg2.connect", side_effect=OSError("Network Error")):
        with pytest.raises(OSError) as exc:
            pool = ConnectionPool(CONNECT_URL, max_connections=5)
            pool.get_connection()
        assert "Network Error" in str(exc.value)
