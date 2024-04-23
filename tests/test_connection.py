import pytest
from psycopg2 import OperationalError, ProgrammingError
from psycopg2.extensions import TRANSACTION_STATUS_ACTIVE

from message_db.connection import ConnectionPool


def test_constructing_connection_pool_from_url(pool):
    assert pool is not None
    assert isinstance(pool, ConnectionPool)


def test_error_on_invalid_url():
    with pytest.raises(ProgrammingError) as exc:
        ConnectionPool("foo://bar")

    assert exc.value.args[0].startswith("invalid dsn")


def test_error_on_invalid_role():
    with pytest.raises(OperationalError) as exc:
        ConnectionPool("postgresql://foo@localhost:5432/postgres")

    assert 'role "foo" does not exist' in exc.value.args[0]


def test_error_on_invalid_max_connections():
    with pytest.raises(ValueError):
        ConnectionPool("postgresql://foo@localhost:5432/postgres", max_connections=-1)


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
