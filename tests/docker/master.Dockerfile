FROM replisome/base

RUN sudo -u postgres mkdir /var/run/postgresql/9.6-main.pg_stat_tmp/

RUN echo 'host all postgres 0.0.0.0/0 trust' \
    >> /etc/postgresql/9.6/main/pg_hba.conf
RUN echo 'host replication postgres 0.0.0.0/0 trust' \
    >> /etc/postgresql/9.6/main/pg_hba.conf

RUN sed -i "s/.*listen_addresses.*/listen_addresses = '0.0.0.0'/" \
    /etc/postgresql/9.6/main/postgresql.conf
RUN sed -i "s/.*wal_level.*/wal_level = logical/" \
    /etc/postgresql/9.6/main/postgresql.conf
RUN sed -i "s/.*max_replication_slots.*/max_replication_slots = 4/" \
    /etc/postgresql/9.6/main/postgresql.conf
RUN sed -i "s/.*max_wal_senders.*/max_wal_senders = 4/" \
    /etc/postgresql/9.6/main/postgresql.conf

ADD ./ /code
WORKDIR /code

RUN make install

CMD ["sudo", "-u", "postgres", "/usr/lib/postgresql/9.6/bin/postgres", \
    "-c", "config_file=/etc/postgresql/9.6/main/postgresql.conf"]
