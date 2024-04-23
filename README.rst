*************
Message-DB-py
*************

Message-DB-py is a Python interface to the MessageDB event store and message store, designed for easy integration into
Python applications.

.. image:: https://github.com/subhashb/message-db-py/actions/workflows/ci.yml/badge.svg?branch=main
    :target: https://github.com/subhashb/message-db-py/actions
    :alt: Build Status
.. image:: https://codecov.io/gh/subhashb/message-db-py/graph/badge.svg?token=QMNUSLN2OM 
    :target: https://codecov.io/gh/subhashb/message-db-py
    :alt: Code Coverage
.. image:: https://img.shields.io/pypi/pyversions/message-db-py.svg
    :target: https://pypi.org/project/message-db-py/
    :alt: Python Version
.. image:: https://badge.fury.io/py/message-db-py.svg
    :target: https://pypi.org/project/message-db-py/
    :alt: PyPI version
.. image:: https://img.shields.io/badge/License-MIT-yellow.svg
    :target: https://opensource.org/licenses/MIT
    :alt: License

Installation
============

Use pip to install:

.. code-block:: shell

    pip install message-db-py

Usage
=====

Here's a quick example of how to publish and read messages using Message-DB-py:

.. code-block:: python

    from message_db.client import MessageDB

    # Initialize the database connection
    mdb = MessageDB("your_connection_string")

    # Write a message
    mdb.write("your_stream_name", "your_message_type", {"data": "value"})

    # Read a message
    message = mdb.read_last_message("your_stream_name")
    print(message)
