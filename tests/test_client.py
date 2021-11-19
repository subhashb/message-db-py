from message_db.client import MessageDB
from message_db.connection import ConnectionPool


def test_client_construction_from_url():
    store = MessageDB.from_url("postgresql://postgres@localhost:5432/postgres")
    assert store is not None

    assert isinstance(store, MessageDB)
    assert isinstance(store.connection_pool, ConnectionPool)

def test_client_construction_from_args():
    store = MessageDB()
    assert store is not None

    assert isinstance(store, MessageDB)
    assert isinstance(store.connection_pool, ConnectionPool)
