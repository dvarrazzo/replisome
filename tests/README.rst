Stuff to test replisome
=======================

The tests available are:

- tests of the PostgreSQL extension: they are in the directories ``sql`` and
  ``expected``. Roughly execute::

    make
    sudo make install
    make installcheck

  you can use a specific ``pg_config`` and ``PG*`` env vars to point to the
  right cluster to test.

- Tests of the Python code: they are in the directory ``pytests``. The tests
  need to connect to databases configured for testing (e.g. configured for
  replication), referred by the env vars ``RS_TEST_SRC_DSN`` and
  ``RS_TEST_TGT_DSN``. In order to test the code during development use::

    python setup.py develop
    python setup.py test

  To test a package installed in the system use instead::

    pip install -r tests/pytests/requirements.txt
    py.test

- Docker Compose setup to test both the PostgreSQL extension and the Python
  code into Docker. The Docker and Compose files are generated from the
  ``templates`` directory by ``ansible`` (to inject the specific Python and
  PostgreSQl versions) into the ``build`` directory. The ``test`` service is
  configured to use the ``master`` and ``slave`` services to run the tests.
  For instance::

    pip install "ansible>=2.4"
    ansible-playbook tests/playbook.yml -e pg_ver=9.6 -e py_ver=3.5

    export COMPOSE_FILE=tests/build/docker-compose.yml
    docker-compose build
    docker-compose up -d
    docker-compose run --rm test make installcheck
    docker-compose run --rm test py.test -v
    docker-compose down

The Travis CI environment is set up to `run a grid of tests`__ using Docker
Compose against the supported Python and PostgreSQL versions.

.. __: https://travis-ci.org/GambitResearch/replisome/
