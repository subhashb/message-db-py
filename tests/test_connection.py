import pytest

from psycopg2 import ProgrammingError, OperationalError
from psycopg2.extensions import TRANSACTION_STATUS_ACTIVE

from message_db.connection import ConnectionPool

def test_constructing_connection_pool_from_url():
    connection_pool = ConnectionPool("postgresql://postgres@localhost:5432/postgres")
    assert connection_pool is not None
    assert isinstance(connection_pool, ConnectionPool)


def test_retrieving_connection_from_pool():
    connection_pool = ConnectionPool("postgresql://postgres@localhost:5432/postgres")
    conn = connection_pool.get_connection()

    assert conn is not None
    assert conn.status == TRANSACTION_STATUS_ACTIVE
    assert len(connection_pool._connection_pool._used) == 1


def test_error_on_invalid_url():
    with pytest.raises(ProgrammingError) as exc:
        ConnectionPool("foo://bar")

    assert exc.value.args[0].startswith("invalid dsn")

def test_error_on_invalid_credentials():
    with pytest.raises(OperationalError) as exc:
        ConnectionPool("postgresql://foo@localhost:5432/postgres")

    assert 'role "foo" does not exist' in exc.value.args[0]
