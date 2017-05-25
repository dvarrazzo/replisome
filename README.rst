==========================================
replisome - handsomely replicate something
==========================================

The project is currently in pre-production phase. We are hacking on it.

**replisome** is a *lightweight*, *flexible*, and *easily configurable* system to
export data changes from PostgreSQL. It allows a client to receive a stream
of changes describing the data manipulation inside the database (INSERT,
UPDATE, DELETE of records) in JSON format for all, or a specified subset of
tables, with the possibility of limiting the columns and rows received.

.. contents::

What can you do with data changes?

- *Replication*: you can apply the changes to another database and obtain a
  copy of the data.
- *Upgrade*, *downgrade*: the database to which you are applying changes could be
  a different version.
- *Export*: the database to which you are applying changes could be something else
  other than PostgreSQL (this is obviously only theoretical, as nobody sane
  would use a different database...)
- *Integrate*: the thing to which you are applying the changes could be *redis*,
  *memcached* or some other key-value store granting fast access to data.
- *Audit*, *logging*: would you like to write all the changes into a file?
- *Email*, *twitter*: get notified when something important changes.
- *Whatevs*: You have a stream of changes from a database, do whatever you
  want with it.

You may have noticed a few easy-to-brag-about buzzwords in the opening
statement: here is why we feel entitled to use them:

- *Lightweight*: replisome is based on `PostgreSQL logical decoding`_, not on
  triggers; as such it doesn't require extra work for the database, such as
  inserting a record in a queue table for every record changed. This makes it
  more efficient than `pgq and londiste`_.

- *Flexible*: replisome allows emitting changes only on specific tables,
  specific columns, or even specific records. The data produced is JSON and the
  system doesn't care about the usage of the data: if used as a replication system
  the database receiving the data doesn't need to have matching tables,
  columns, or data types (as pglogical_ requires).

- *Easy to configure*: the entire configuration, from the selection of the
  data to export to its usage, is a parameter of the script consuming the
  data; there is no persistent configuration (nodes, subscribers, replication
  sets...). Changing configuration only requires changing the configuration
  file the running consumer script is using, and then stopping and restarting it.

.. _pgq and londiste: skytools_
.. _skytools: http://pgfoundry.org/projects/skytools
.. _PostgreSQL logical decoding: https://www.postgresql.org/docs/current/static/logicaldecoding-explanation.html
.. _pglogical: https://www.2ndquadrant.com/en/resources/pglogical/

**replisome** is not a complete replication solution: it doesn't deal with
truncate, DDL language, sequences replication, or conflicts. If you are looking
for something like that take a look at pglogical_ instead. What it aims to be
is a more flexible tool for data integration.


System description
==================

The system is composed of two main parts:

- `The sender`__ is a PostgreSQL logical replication decoder plugin that can
  be widely configured in order to choose what data to emit and how.

- `The receiver`__ is an easy to extend Python framework allowing manipulation
  and consumption of data produced by a sender. It is easy to write your
  own extensions to this framework, or ditch it altogether and make direct use
  of the data produced by the sender.

.. __: `Decoding plugin`_
.. __: `Consumer Framework`_


Decoding plugin
===============

The decoding plugin is the bit that lives in the PostgreSQL server used as a
data source. Please refer to the documentation__ for an introduction to
logical decoding.

.. __: `PostgreSQL logical decoding`_

Requirements
------------

The data source must be PostgreSQL running at least version 9.4 or newer.


Build and Install
-----------------

TODO: ``pgxn install replisome``.

This thing will be packaged as an extension, have a version number, be
released on PGXN... but currently it is only available `on github`__.

.. __: https://github.com/GambitResearch/replisome

The extension should be compiled and installed in a PostgreSQL installation,
after which it will be available in the database clusters run by that
installation.

In order to build the extension you will need a C compiler, the PostgreSQL
server development packages and maybe something else that you could easily
find by googling the problem.

.. code:: console

    $ git clone https://github.com/GambitResearch/replisome.git
    $ cd replisome
    $ make PG_CONFIG=/path/to/bin/pg_config
    $ sudo make PG_CONFIG=/path/to/bin/pg_config install


Configuration
-------------

The cluster must be configured to use logical replication: you need to add
the following parameters to ``postgresql.conf``::

    wal_level = logical
    max_replication_slots = 1       # at least
    max_wal_senders = 1             # at least

After changing these parameters a restart is needed.

You will also need to set permissions in ``pg_hba.conf`` to allow
replication connections ::

    local    replication     myuser                     trust
    host     replication     myuser     10.1.2.3/32     trust

Every replisome consumer must connect to a `replication slot`_, which will
hold the state of the replication client (so that a stopped consumer will not
miss the data: on restart it will pick up from where it left off). You can
create a replication slot using:

.. code:: psql

    =# select pg_create_logical_replication_slot('MY NAME', 'replisome');

The name is what will be used by the client to connect to a specific slot.

.. _replication slot: https://www.postgresql.org/docs/current/static/warm-standby.html#STREAMING-REPLICATION-SLOTS


Examples
--------

There are a few ways to obtain the changes (JSON objects) from the
**replisome** plugin:

* using `SQL functions`__ such as ``pg_logical_slot_get_changes()``
* using pg_recvlogical__ from command line.
* using `psycopg replication protocol support`__.
* using the `replisome Python package`__.

.. __: https://www.postgresql.org/docs/9.4/static/functions-admin.html#FUNCTIONS-REPLICATION-TABLE
.. __: https://www.postgresql.org/docs/current/static/app-pgrecvlogical.html
.. __: http://initd.org/psycopg/docs/advanced.html#replication-protocol-support
.. __: `Consumer Framework`_


Examples using ``pg_recvlogical``
---------------------------------

You are ready to try replisome. In one terminal create a replication slot and
start a replica:

.. code:: console

    $ pg_recvlogical -d postgres --slot test_slot --create-slot -P replisome
    $ pg_recvlogical -d postgres --slot test_slot --start -o pretty-print=1 -f -

In another terminal connect to the database and enter some commands:

.. code:: psql

    =# create table test (
       id serial primary key, data text, ts timestamptz default now());
    CREATE TABLE

    =# insert into test default values;
    INSERT 0 1
    =# insert into test (data) values ('hello');
    INSERT 0 1

    =# begin;
    BEGIN
    *=# update test set data = 'world' where id = 2;
    UPDATE 1
    *=# delete from test where id = 1;
    DELETE 1
    *=# commit;
    COMMIT


The streaming connection should display a JSON description of the operations
performed:

.. code:: json

    {
        "tx": [
            {
                "op": "I",
                "schema": "public",
                "table": "test",
                "colnames": ["id", "data", "ts"],
                "coltypes": ["int4", "text", "timestamptz"],
                "values": [1, null, "2017-05-13 13:15:28.052318+01"]
            }
        ]
    }
    {
        "tx": [
            {
                "op": "I",
                "schema": "public",
                "table": "test",
                "values": [2, "hello", "2017-05-13 13:15:35.140594+01"]
            }
        ]
    }
    {
        "tx": [
            {
                "op": "U",
                "schema": "public",
                "table": "test",
                "values": [2, "world", "2017-05-13 13:15:35.140594+01"],
                "keynames": ["id"],
                "keytypes": ["int4"],
                "oldkey": [2]
            }
            ,{
                "op": "D",
                "schema": "public",
                "table": "test",
                "oldkey": [1]
            }
        ]
    }


Options
-------

The plugin output content and format is configured by several options passed
to the START_REPLICATION__ command (e.g. using the ``-o`` option of
``pg_recvlogical``, the psycopg `start_replication()`__ method etc).

.. __: https://www.postgresql.org/docs/9.4/static/protocol-replication.html
.. __: http://initd.org/psycopg/docs/extras.html#psycopg2.extras.ReplicationCursor.start_replication

``pretty-print`` [``bool``] (default: ``false``)
    Add whitespace to the output for readibility.

``include`` [``json``]
    Choose which tables and filter content from those tables. This command
    together with ``exclude`` can be used several times: each table will be
    considered for inclusion or exclusion by matching it against all the
    commands specified in order from top to bottom. The last matching command
    will override previous commands. (e.g. you may exclude an entire schema and
    then include only one specific table from it).

    The parameter is a JSON object which may contain the following keys:

    - ``table``: match a table with this name, in any schema
    - ``tables``: match all the tables whose name matches a regular
      expression, in any schema
    - ``schema``: match all the tables in a schema
    - ``schemas``: match all the tables in all the schemas whose name matches
      a regular expression

    These keys will establish if a table matches the configuration object. At
    least one schema or a table must be specified. The following options can
    be specified too, and they will affect any table included:

    - ``columns``: only emit the columns specified (as a JSON array)
    - ``skip_columns``: don't emit the columns specified (as a JSON array)
    - ``where``: only emit the row matching the condition specified as an SQL
      expression matching the table columns, like in a ``CHECK`` clause.

    Example (as ``pg_recvlogical`` option)::

        -o '{"tables": "^test.*", "skip_columns": ["ts", "wat"], "where": "id % 2 = 0"}'

``exclude`` [``json``]
    Choose which tables to exclude. The format is the same as ``include`` but
    only the tables/schemas can be specified, no rows or columns.

``include-xids`` [``bool``] (default: ``false``)
    If ``true``, include the id of each transaction::

        {
            "xid": 5360,
            "tx": [
                {   ...

``include-lsn`` [``bool``] (default: ``false``)
    Include the Log Sequence Number of the transaction::

        {
            "nextlsn": "0/3784C40",
            "tx": [
                {   ...


``include-timestamp`` [``bool``] (default: ``false``)
    Include the commit time of the transaction::

        {
            "timestamp": "2017-05-13 03:19:29.828474+01",
            "tx": [
                {   ...

``include-schemas`` [``bool``] (default: ``true``)
    Include the schema name of the tables.

``include-types`` [``bool``] (default: ``true``)
    Include the types of the table columns.

``include-empty-xacts`` [``bool``] (default: ``false``)
    If ``true``, send information about transactions not containing data
    changes (e.g. ones only performing DDL statements). Only the metadata (e.g.
    time, txid) of the transaction are sent.

``write-in-chunks`` [``bool``] (default: ``false``)
    If ``true``, data may be sent in several chunks instead of a single
    message for the entire transaction. Please note that a single chunk may
    not be a valid JSON document and the client is responsible for aggregation
    of received parts.


Consumer Framework
==================

The consumer framework consists of a script entry point called ``replisome``,
taking a configuration file to describe where to read the data, how to
transform it and what to do with it. Any Python callable can be used to
transform and consume data. A few useful objects are provided as part of the
package.


Requirements
------------

Python 2.7 or later [TODO: python 3]


Installation
------------

TODO: ``pip install replisome``

Currently, clone the repos and run ``python setup.py install``


Usage
-----

The ``replisome`` command line parameters are:

.. parsed-literal::

    usage: replisome [-h] [--dsn *DSN*] [--slot *SLOT*] [-v | -q] [*configfile*]

    Receive data from a database, and do something with it.

    positional arguments:
      *configfile*     configuration file to parse; if not specified print to
                     stderr

    optional arguments:
      -h, --help     show this help message and exit
      --dsn *DSN*      database to read from (overrides the config file)
      --slot *SLOT*    the replication slot to connect to (overrides the config
                     file)
      -v, --verbose  print debugging information to stderr
      -q, --quiet    minimal output on stderr

If *configfile* is not specified, ``--dsn`` and ``--slot`` must be used: the
script will print on stdout all the changes read in the database connected.
More interesting stuff can be done specifying a *configfile*.


Configuration
-------------

The ``replisome`` configuration file must be a YAML file describing a
process pipeline (one receiver, zero or more filters, one consumer). Example:

.. code:: yaml

    receiver:
        class: JsonReceiver
        dsn: "dbname=source host=sourcedb"
        slot: someslot
        options:
            pretty_print: false
            includes:
              - schema: myapp
                tables: '^contract(_expired_\d{6})?$'
                where: "seller in ('alice', 'bob')"
              - schema: myapp
                table: account
                skip_columns: [password]

    filters:
      - class: TableRenamer
        options:
            from_schema: myapp
            to_schema: otherapp

    consumer:
        class: DataUpdater
        options:
            dsn: "dbname=target host=targetdb"
            skip_missing_columns: true

Every object is specified by a ``class`` entry, which should be the name of
one of the `objects provided by the package`__ or a fully qualified Python
callable (e.g. ``mypackage.mymodule.MyClass``). In either case the object will
be called passing the contents of the ``options`` object as keyword
arguments.

Receivers must subclass the TODO class; filters and consumers can be any
callable object (i.e. the object returned by the ``class`` specified in the
configuration file must be a callable itself): filters will take a JSON
message as input (decoded as Python objects) and should return a new message,
which will be passed to the following filters and eventually to the consumer.
If a filter returns ``None`` the message is dropped. The consumer must be a
callable taking a message too. The return value is discarded.

Only after the consumer has processed a message will the server receive a
notification that the message has been consumed. If processing is interrupted
for any reason (e.g. user interruption, network error, Python exception), then
replication will restart from the point where it was interrupted.

.. __: https://github.com/GambitResearch/replisome/tree/master/replisome


License
=======

| Copyright (c) 2013-2017, Euler Taveira de Oliveira
| Copyright (c) 2017, Gambit Research Ltd.
| All rights reserved.

Redistribution and use in source and binary forms, with or without
modification, are permitted provided that the following conditions are met:

* Redistributions of source code must retain the above copyright notice,
  this list of conditions and the following disclaimer.

* Redistributions in binary form must reproduce the above copyright notice,
  this list of conditions and the following disclaimer in the documentation
  and/or other materials provided with the distribution.

* Neither the name of Gambit Research Ltd. nor the names of its contributors
  may be used to endorse or promote products derived from this software
  without specific prior written permission.

THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE
LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR
CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF
SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS
INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN
CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE)
ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE
POSSIBILITY OF SUCH DAMAGE.
