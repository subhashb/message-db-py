import psycopg2
import pytest

from message_db.client import MessageDB
from message_db.connection import ConnectionPool


@pytest.fixture(scope="session")
def pool():
    pool = ConnectionPool("postgresql://message_store@localhost:5432/message_store")
    yield pool

    pool.closeall()


@pytest.fixture(scope="session")
def client():
    return MessageDB.from_url("postgresql://message_store@localhost:5432/message_store")


@pytest.fixture(autouse=True)
def clean_up(pool):
    yield

    # Truncate and empty messages table
    conn = psycopg2.connect(
        dbname="message_store", user="postgres", port=5432, host="localhost"
    )

    cursor = conn.cursor()
    cursor.execute("TRUNCATE message_store.messages RESTART IDENTITY;")

    conn.commit()  # Apparently, psycopg2 requires a `commit` even if its a `TRUNCATE` command

    cursor.close()
    conn.close()
