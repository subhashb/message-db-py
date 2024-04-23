# message-db-py

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

## Usage

Here's a quick example of how to publish and read messages using Message-DB-py:

```python
from message_db.client import MessageDB

# Initialize the database connection
mdb = MessageDB("your_connection_string")

# Write a message
mdb.write("your_stream_name", "your_message_type", {"data": "value"})

# Read a message
message = mdb.read_last_message("your_stream_name")
print(message)
```