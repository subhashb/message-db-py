Release History
===============

0.3.5
-----

* Return ``None`` for a message whose ``data`` is NULL, instead of raising ``TypeError``
* Order ``$all`` reads by ``global_position``

0.3.4
-----

* Relax the Python version constraint from ``^3.11`` to ``>=3.11``

0.3.3
-----

* Fix the thread-unsafe connection pool and connection leaks in read methods

0.3.2
-----

* Leave empty identifiers out of ``stream_identifiers``

0.3.1
-----

* Add ``stream_identifiers`` to list the unique aggregate identifiers in a category

0.3.0
-----

* Add consumer group support for reading categories
* Add Python 3.14 support
* Require Python 3.11 or later
* Upgrade all dependencies

0.2.0
-----

* Add documentation and bring test coverage to 100%
* Support Python 3.9 and later

0.1.4
-----

* Fix installing the package for tests

0.1.3
-----

* Upgrade packages and documentation
* Change package structure to `src` and `tests` folders

0.1.2
-----

* Enable reading from all streams

0.1.1
-----

* Enable editable installation with Poetry
* Return data as json instead of string

0.1.0
-----

* First Release
