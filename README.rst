==========================================
replisome - handsomely replicate something
==========================================

The project is currently in pre-production phase. We are hacking on it.

**replisome** is a *lightweight*, *flexible*, *easy to configure* system to
export data changes from PostgreSQL.  It allows a client to receive a stream
of changes describing the data manipulation inside the database (INSERT,
UPDATE, DELETE of records) in JSON format, for all or a specified subset of
tables, with the possibility of limiting the columns and rows received.

.. contents:: Table of Contents

What can you do with data changes?

- *Replication*: you can apply the changes to another database and obtain a
  copy of the data.
- *Upgrade*, *downgrade*: the database you are applying changes could be a
  different version.
- *Export*: the database you are applying changes could be something else than
  PostgreSQL (this is more a theoretical possibility than else, as nobody sane
  would use a different database...)
- *Integrate*: the thing you are applying the changes could be *redis*,
  *memcached* or some other key-value store granting fast access to data.
- *Audit*, *logging*: would you like to write all the changes into a file?
- *Email*, *twitter*: just in case you want to make things happen when a
  certain record is created.
- *Etcetera*. You have a stream of changes from a database: do whatever you
  want with it.

You may have noticed a few easy-to-brag-about buzzwords in the opening
statement: here is why we feel entitled to use them:

- *Lightweight*: replisome is based on `PostgreSQL logical decoding`_, not on
  triggers; as such it doesn't require extra work for the database, such as
  inserting a record in a queue table for every record changed. This makes it
  more efficient than e.g. `pgq and londiste`_.

- *Flexible*: replisome allows emitting changes only on specific tables,
  specific columns, specific records. The data produced is JSON and the system
  doesn't care about the usage of the data: if used as replication system the
  database receiving the data doesn't require to have matching tables,
  columns, or data types.

- *Easy to configure*: the entire configuration, from the selection of the
  data to export to its usage, is a parameter of the script consuming the
  data; there is no persistent configuration (nodes, subscribers, replication
  sets...). Changing configuration only requires changing the configuration
  file the running consumer script is using, stopping and restarting it.

.. _pgq and londiste: skytools_
.. _skytools: http://pgfoundry.org/projects/skytools
.. _PostgreSQL logical decoding: https://www.postgresql.org/docs/current/static/logicaldecoding-explanation.html
.. _pglogical: https://www.2ndquadrant.com/en/resources/pglogical/

**replisome** is not a complete replication solution: it doesn't deal with
truncate, DDL language, sequences replication, conflicts. If you are looking
for something like that take a look at pglogical_ instead. What aims to be is
a more flexible tool for data integration.


System description
==================

The system is composed by two main parts:

- `The sender`__ is a PostgreSQL logical replication decoder plugin that can
  be widely configured in order to choose what data to emit and how.

- `The receiver`__ is an easy to extend Python framework allowing to
  manipulate and consume data produced by a sender. It is easy to write your
  own extensions to this framework, or ditch it altogether and use directly
  the data produced by the sender.

.. __: `Decoding plugin`_
.. __: `Consumer Framework`_


Decoding plugin
===============

The decoding plugin is the bit that lives in the PostgreSQL server used as
data source. Please refer to the documentation__ for an introduction about
logical decoding.

.. __: `PostgreSQL logical decoding`_

Requirements
------------

The data source must be PostgreSQL 9.4 or following versions.


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
server development packages and maybe something else that google will friendly
tell you. ::

    $ git clone https://github.com/GambitResearch/replisome.git
    $ cd replisome
    $ make PG_CONFIG=/path/to/bin/pg_config
    $ sudo make PG_CONFIG=/path/to/bin/pg_config install


Configuration
-------------

The cluster must be configured to use logical replication: you need to set up
at least two parameters into ``postgresql.conf``::

    wal_level = logical
    max_replication_slots = 1       # at least
    max_wal_senders = 1             # at least

After changing these parameters, a restart is needed.

You will also need enough permission in the ``pg_hba.conf`` to allow
replication connections, e.g. ::

    local    replication     myuser                     trust
    host     replication     myuser     10.1.2.3/32     trust

Every replisome consumer must connect to a `replication slot`_, which will
hold the state of the replication client (so that a consumer stopped will not
miss the data: on restart it will pick the data from where it left). You can
create a replication slot using::

    select pg_create_logical_replication_slot('MY NAME', 'replisome');

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
start a replica::

    $ pg_recvlogical -d postgres --slot test_slot --create-slot -P replisome
    $ pg_recvlogical -d postgres --slot test_slot --start -o pretty-print=1 -f -

In another terminal connect to the database and enter some commands::

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


The streaming connection should display a description of the operations
performed::

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
    Add whitespaces in the output for readibility.

``include`` [``json``]
    Choose what tables and what content to see of these tables. The command,
    together with ``exclude``, can be used several times: each table will be
    considered for inclusion or exclusion by matching it against all the
    commands specified in order. The last matching command will take effect
    (e.g. you may exclude an entire schema and then include only one specific
    table into it).

    The parameter is a JSON object which may contain the following keys:

    - ``table``: match a table with this name, in any schema
    - ``tables``: match all the tables whose name matches a regular
      expression, in any schema
    - ``schema``: match all the tables in a schema
    - ``schemas``: match all the tables in all the schemas whose name matches
      a regular expression

    These keys will establish if a table matches or not the configuration
    object.  At least a schema or a table must be specified. The following
    options can be specified too, and they will affect any table whose
    inclusion is decided by the object:

    - ``columns``: only emit the columns specified (as a JSON array)
    - ``skip_columns``: don't emit the columns specified
    - ``where``: only emit the row matching the condition specified as a SQL
      expression matching the table columns, like in a ``CHECK`` clause.

    Example (as ``pg_recvlogical`` option)::

        -o ' {"tables": "^test.*", "skip_columns": ["ts", "wat"], "where": "id % 2 = 0"}'

``exlcude`` [``json``]
    Choose what table to exclude. The format is the same of ``include`` but
    only the tables/schems can be specified, no rows or columns.

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
    changes (e.g. ones only performing DDL statements. Only the metadata (e.g.
    time, txid) of the transaction are sent.

``write-in-chunks`` [``bool``] (default: ``false``)
    If ``true``, data may be sent in several chunks instead of a single
    message for the entire transaction.  Please note that a single chunk may
    not be a valid JSON document and the client is responsible to aggregate
    the parts received.


Consumer Framework
==================

TODO

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
