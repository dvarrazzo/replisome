EXTENSION = replisome
MODULE_big = $(EXTENSION)

OBJS = src/replisome.o src/executor.o src/includes.o src/jsonbutils.o \
		src/reldata.o

REGRESS = --inputdir=tests \
		init insert1 cmdline update1 update2 update3 update4 delete1 delete2 \
		delete3 delete4 include repschema row_filter savepoint specialvalue \
		toast bytea

# Grab the extension version (for extension upgrade) from control file
EXTVER = $(shell grep 'default_version' $(EXTENSION).control \
		 | sed "s/\([^']\+'\)\([^']\+\)\('.*\)/\2/")

# Grab the replisome version (to check protocol compatibility) from python code
RSVER = $(shell grep '^VERSION' replisome/version.py \
		 | sed "s/\([^'\"]\+'\)\([^'\"]\+\)\('.*\)/\2/")

DATA_built = sql/$(EXTENSION)--$(EXTVER).sql

PG_CPPFLAGS = -DREPLISOME_VERSION=$(RSVER)

PG_CONFIG = pg_config
PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)

sql/$(EXTENSION)--$(EXTVER).sql: sql/$(EXTENSION).sql
	cat $< > $@


# make installcheck
#
# It can be run but you need to add the following parameters to
# postgresql.conf:
#
# wal_level = logical
# max_replication_slots = 4
#
# Also, you should start the server before executing it.
