# message-db-py

Message DB is a fully-featured event store and message store implemented in
PostgreSQL for Pub/Sub, Event Sourcing, Messaging, and Evented Microservices
applications.

`message-db-py` is a Python interface to the Message DB event store and message 
store, designed for easy integration into Python applications.

[![Build Status](https://github.com/subhashb/message-db-py/actions/workflows/ci.yml/badge.svg?branch=main)](https://github.com/subhashb/message-db-py/actions)
[![Code Coverage](https://codecov.io/gh/subhashb/message-db-py/graph/badge.svg?token=QMNUSLN2OM)](https://codecov.io/gh/subhashb/message-db-py)
[![Python Version](https://img.shields.io/pypi/pyversions/message-db-py.svg)](https://pypi.org/project/message-db-py/)
[![PyPI version](https://badge.fury.io/py/message-db-py.svg)](https://pypi.org/project/message-db-py/)
[![License](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)

## Installation

Use pip to install:

```shell
$ pip install message-db-py
```

## Setting up Message DB database

Clone the Message DB repository to set up the database:

```shell
$ git clone git@github.com:message-db/message-db.git
```

More detailed instructions are in the [Installation](https://github.com/message-db/message-db?tab=readme-ov-file#installation)
section of Message DB repo.

Running the database installation script creates the database, schema, table,
indexes, functions, views, types, a user role, and limit the user's privileges
to the message store's public interface.

The installation script is in the database directory of the cloned Message DB
repo. Change directory to the message-db directory where you cloned the repo,
and run the script:

```shell
$ database/install.sh
```

Make sure that your default Postgres user has administrative privileges.

### Database Name

By default, the database creation tool will create a database named
`message_store`.

If you prefer either a different database name, you can override the name
using the `DATABASE_NAME` environment variable.

```shell
$ DATABASE_NAME=some_other_database database/install.sh
```

### Uninstalling the Database

If you need to drop the database (for example, on a local dev machine):

``` bash
$ database/uninstall.sh
```

If you're upgrading a previous version of the database:

``` bash
$ database/update.sh
```

## Docker Image

You can optionally use a Docker image with Message DB pre-installed and ready
to go. This is especially helpful to run test cases locally.

The docker image is available in [Docker Hub](https://hub.docker.com/r/ethangarofolo/message-db).
The source is in [Gitlab](https://gitlab.com/such-software/message-db-docker)

## Usage

The complete user guide for Message DB is available at 
[http://docs.eventide-project.org/user-guide/message-db/]
(http://docs.eventide-project.org/user-guide/message-db/).

Below is documentation for methods exposed through the Python API.

### Quickstart

Here's a quick example of how to publish and read messages using Message-DB-py:

```python
from message_db import MessageDB

# Initialize the database connection
store = MessageDB(CONNECTION_URL)

# Write a message
store.write("user_stream", "register", {"name": "John Doe"})

# Read a message
message = store.read_last_message("user_stream")
print(message)
```

## Primary APIs

- [Write Messages](#write)
- [Read Messages](#read-messages-from-a-stream-or-category)
- [Read Last Message from stream](#read-last-message-from-stream)

### Write messages

The `write` method is used to append a new message to a specified stream within
the message database. This method ensures that the message is written with the
appropriate type, data, and metadata, and optionally, at a specific expected
version of the stream.

```python
def write(
    self,
    stream_name: str,
    message_type: str,
    data: Dict,
    metadata: Dict | None = None,
    expected_version: int | None = None,
) -> int:
    """Write a message to a stream."""
```

#### Parameters

- `stream_name` (`str`): The name of the stream to which the message will be
written. This identifies the logical series of messages.
- `message_type` (`str`): The type of message being written. Typically, this
reflects the nature of the event or data change the message represents.
- `data` (`Dict`): The data payload of the message. This should be a dictionary
containing the actual information the message carries.
- `metadata` (`Dict` | `None`): Optional. Metadata about the message, provided as a
dictionary. Metadata can include any additional information that is not part of
the - main data payload, such as sender information or timestamps.
Defaults to None.
- `expected_version` (`int` | `None`): Optional. The version of the stream where the
client expects to write the message. This is used for concurrency control and
ensuring the integrity of the stream's order. Defaults to `None`.

#### Returns

- `position` (`int`): The position (or version number) of the message in the
stream after it has been successfully written.

```python
message_db = MessageDB(connection_pool=my_pool)
stream_name = "user_updates"
message_type = "UserCreated"
data = {"user_id": 123, "username": "example"}
metadata = {"source": "web_app"}

position = message_db.write(stream_name, message_type, data, metadata)

print("Message written at position:", position)
```

---

### Read messages from a stream or category

The `read` method retrieves messages from a specified stream or category. This
method supports flexible query options through a direct SQL parameter or by
determining the SQL based on the stream name and its context
(stream vs. category vs. all messages).

```python
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
```

#### Parameters

- `stream_name` (`str`): The identifier for the stream or category from which
messages are to be retrieved. Special names like "$all" can be used to fetch
messages across all streams.
- `sql` (`str` | `None`, optional): An optional SQL query string that if
provided, overrides the default SQL generation based on the stream_name.
If None, the SQL is automatically generated based on the stream_name value.
Defaults to None.
- `position` (`int`, optional): The starting position in the stream or category
from which to begin reading messages. Defaults to 0.
- `no_of_messages` (`int`, optional): The maximum number of messages to
retrieve. Defaults to 1000.

#### Returns

- List[Dict[str, Any]]: A list of messages, where each message is
represented as a dictionary containing details such as the message ID,
stream name, type, position, global position, data, metadata, and timestamp.

```python
message_db = MessageDB(connection_pool=my_pool)
stream_name = "user-updates"
position = 10
no_of_messages = 50

# Reading from a specific stream
messages = message_db.read(stream_name, position=position, no_of_messages=no_of_messages)

# Custom SQL query
custom_sql = "SELECT * FROM get_stream_messages(%(stream_name)s, %(position)s, %(batch_size)s);"
messages = message_db.read(stream_name, sql=custom_sql, position=position, no_of_messages=no_of_messages)

for message in messages:
    print(message)
```

---

### Read Last Message from stream

The `read_last_message` method retrieves the most recent message from a
specified stream. This method is useful when you need the latest state or
event in a stream without querying the entire message history.

```python
def read_last_message(self, stream_name: str) -> Dict[str, Any] | None:
    """Read the last message from a stream."""
```

#### Parameters

- `stream_name` (`str`): The name of the stream from which the last message is to be
retrieved.

#### Returns

- `Dict`[`str`, `Any`] | `None`: A dictionary representing the last message 
in the specified stream. If the stream is empty or the message does not exist,
`None` is returned.

```python
message_db = MessageDB(connection_pool=my_pool)
stream_name = "user_updates"

# Reading the last message from a stream
last_message = message_db.read_last_message(stream_name)

if last_message:
    print("Last message data:", last_message)
else:
    print("No messages found in the stream.")
```

---

## Utility APIs

- [Read Stream](#read-stream)
- [Read Category](#read-category)
- [Write Batch](#write-batch)

### Read Stream

The `read_stream` method retrieves a sequence of messages from a specified stream
within the message database. This method is specifically designed to fetch
messages from a well-defined stream based on a starting position and a
specified number of messages.

```python
def read_stream(
    self, stream_name: str, position: int = 0, no_of_messages: int = 1000
) -> List[Dict[str, Any]]:
    """Read messages from a stream.

    Returns a list of messages from the stream starting from the given position.
    """
```

#### Parameters

- `stream_name` (`str`): The name of the stream from which messages are to be
retrieved. This name must include a hyphen (-) to be recognized as a valid
stream identifier.
- `position` (`int`, optional): The zero-based index position from which to start
reading messages. Defaults to 0, which starts reading from the beginning of
the stream.
- `no_of_messages` (`int`, optional): The maximum number of messages to retrieve
from the stream. Defaults to 1000.

#### Returns

- `List`[`Dict`[`str`, `Any`]]: A list of dictionaries, each representing a message
retrieved from the stream. Each dictionary contains the message details
structured in key-value pairs.

#### Exceptions

- `ValueError`: Raised if the provided stream_name does not contain a hyphen
(-), which is required to validate the name as a stream identifier.

```python
message_db = MessageDB(connection_pool=my_pool)
stream_name = "user-updates-2023"
position = 0
no_of_messages = 100

messages = message_db.read_stream(stream_name, position, no_of_messages)

for message in messages:
    print(message)
```

---

### Read Category

The `read_category` method retrieves a sequence of messages from a specified
category within the message database. It is designed to fetch messages based
on a category identifier, starting from a specific position, and up to a
defined limit of messages.

```python
def read_category(
    self, category_name: str, position: int = 0, no_of_messages: int = 1000
) -> List[Dict[str, Any]]:
    """Read messages from a category.

    Returns a list of messages from the category starting from the given position.
    """
```

#### Parameters

- `category_name` (`str`): The name of the category from which messages are to be
retrieved. This identifier should not include a hyphen (-) to validate it as
a category name.
- `position` (`int`, optional): The zero-based index position from which to start
reading messages within the category. Defaults to 0.
- `no_of_messages` (`int`, optional): The maximum number of messages to retrieve
from the category. Defaults to 1000.

#### Returns

- List[Dict[str, Any]]: A list of dictionaries, each representing a message.
Each dictionary includes details about the message such as the message ID,
stream name, type, position, global position, data, metadata, and time of
creation.

#### Exceptions

- `ValueError`: Raised if the provided category_name contains a hyphen (-),
which is not allowed for category identifiers and implies a misunderstanding
between streams and categories.

```python
message_db = MessageDB(connection_pool=my_pool)
category_name = "user_updates"
position = 0
no_of_messages = 100

# Reading messages from a category
messages = message_db.read_category(category_name, position, no_of_messages)

for message in messages:
    print(message)
```

---

### Write Batch

The `write_batch` method is designed to write a series of messages to a
specified stream in a batch operation. It ensures atomicity in writing
operations, where all messages are written in sequence, and each subsequent
message can optionally depend on the position of the last message written.
This method is useful when multiple messages need to be written as a part of a
single transactional context.

```python
def write_batch(
    self, stream_name, data, expected_version: int | None = None
) -> int:
    """Write a batch of messages to a stream."""
```

#### Parameters

- `stream_name` (`str`): The name of the stream to which the batch of messages
will be written.
- `data` (`List`[`Tuple`[`str`, `Dict`, `Dict` | `None`]]): A list of tuples,
where each tuple represents a message. The tuple format is (message_type, data,
metadata), with metadata being optional.
- `expected_version` (`int` | `None`, optional): The version of the stream
where the batch operation expects to start writing. This can be used for
concurrency control to ensure messages are written in the expected order.
Defaults to None.

#### Returns

- `position` (`int`): The position (or version number) of the last message
written in the stream as a result of the batch operation.

```python
message_db = MessageDB(connection_pool=my_pool)
stream_name = "order_events"
data = [
    ("OrderCreated", {"order_id": 123, "product_id": 456}, None),
    ("OrderShipped",
        {"order_id": 123, "shipment_id": 789},
        {"priority": "high"}
    ),
    ("OrderDelivered", {"order_id": 123, "delivery_date": "2024-04-23"}, None)
]

# Writing a batch of messages to a stream
last_position = message_db.write_batch(stream_name, data)

print(f"Last message written at position: {last_position}")
```

---

## License

[MIT](https://github.com/subhashb/message-db-py/blob/main/LICENSE)