MongoDB Oplog Publisher
=======================

Simple utility, which subscribe to mongodb oplog and publish its entries to RabbitMQ.
Currently supported only on Ubuntu > 12.04, with python2.7.

Installation
------------

.. code-block:: sh

    `pip install oplogstream`

Configuration
-------------

Config file lookup take place in /etc/oplogstream/, /etc/oplogstream/conf.d/ and package directory.

Usage
-----

.. code-block:: sh

    `oplogstreamd start|stop|restart|run`
