FROM ubuntu:16.04

RUN apt-key adv --keyserver hkp://keyserver.ubuntu.com:80 \
    --recv-keys B97B0AFCAA1A47F044F244A07FCC7D46ACCC4CF8
RUN echo "deb http://apt.postgresql.org/pub/repos/apt/ xenial-pgdg main" \
    > /etc/apt/sources.list.d/pgdg.list

RUN apt-get update

RUN apt-get install -y build-essential sudo \
    postgresql-9.6 postgresql-client-9.6 postgresql-server-dev-9.6
