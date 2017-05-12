MODULE_big = replisome

OBJS = src/replisome.o src/executor.o src/includes.o src/jsonbutils.o \
		src/reldata.o

REGRESS = --inputdir=tests \
		init insert1 cmdline update1 update2 update3 update4 delete1 delete2 \
		delete3 delete4 include repschema row_filter savepoint specialvalue \
		toast bytea

PG_CONFIG = pg_config
PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)

# make installcheck
#
# It can be run but you need to add the following parameters to
# postgresql.conf:
#
# wal_level = logical
# max_replication_slots = 4
#
# Also, you should start the server before executing it.
