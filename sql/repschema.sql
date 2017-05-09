\set VERBOSITY terse

-- predictability
SET synchronous_commit = on;

DROP TABLE IF EXISTS table_with_pk;
DROP TABLE IF EXISTS table_without_pk;
DROP TABLE IF EXISTS table_with_unique;

CREATE TABLE table_with_pk (
	i int,
	a text,
	b text,
	PRIMARY KEY(i, a)
);

CREATE TABLE table_without_pk (
	i int,
	a text,
	b text
);

CREATE TABLE table_with_unique (
	i int not null,
	a text not null,
	b text,
	UNIQUE(i, a)
);

SELECT 'init' FROM pg_create_logical_replication_slot('regression_slot', 'replisome');

-- Schema omitted on table repeated
insert into table_with_pk values (1, 'a', 'b');
insert into table_with_pk values (2, 'a', 'b');
update table_with_pk set a = 'A' where i = 2;
update table_with_pk set b = 'B' where i = 2;
delete from table_with_pk where i = 1;
SELECT data FROM pg_logical_slot_get_changes('regression_slot', NULL, NULL, 'pretty-print', '1');

-- Schema repeated on new plugin chunk
insert into table_with_pk values (3, 'a', 'b');
insert into table_with_pk values (4, 'a', 'b');
update table_with_pk set a = 'A' where i = 4;
update table_with_pk set b = 'B' where i = 4;
delete from table_with_pk where i = 3;
SELECT data FROM pg_logical_slot_get_changes('regression_slot', NULL, NULL, 'pretty-print', '1');

-- Schema change detected before new chunk
alter table table_with_pk add c text;
insert into table_with_pk values (5, 'a', 'b', 'c');
insert into table_with_pk values (6, 'a', 'b', 'c');
SELECT data FROM pg_logical_slot_get_changes('regression_slot', NULL, NULL, 'pretty-print', '1');

-- Schema change detected within a new chunk
insert into table_with_pk values (7, 'a', 'b', 'c');
insert into table_with_pk values (8, 'a', 'b', 'c');
alter table table_with_pk add d text;
insert into table_with_pk values (9, 'a', 'b', 'c', 'd');
insert into table_with_pk values (10, 'a', 'b', 'c', 'd');
SELECT data FROM pg_logical_slot_get_changes('regression_slot', NULL, NULL, 'pretty-print', '1');

-- Schema change detected within a transaction
begin;
insert into table_with_pk values (11, 'a', 'b', 'c', 'd');
insert into table_with_pk values (12, 'a', 'b', 'c', 'd');
alter table table_with_pk drop d;
insert into table_with_pk values (13, 'a', 'b', 'c');
insert into table_with_pk values (14, 'a', 'b', 'c');
commit;
SELECT data FROM pg_logical_slot_get_changes('regression_slot', NULL, NULL, 'pretty-print', '1');

-- Change to the pkey
begin;
insert into table_with_pk values (15, 'a', 'b', 'c');
update table_with_pk set a = 'A' where i = 15;
delete from table_with_pk where i = 15;
alter table table_with_pk drop a;
alter table table_with_pk add primary key (i);
insert into table_with_pk values (16, 'b', 'c');
update table_with_pk set b = 'B' where i = 16;
delete from table_with_pk where i = 16;
commit;
SELECT data FROM pg_logical_slot_get_changes('regression_slot', NULL, NULL, 'pretty-print', '1');


-- Schema omitted on table repeated
insert into table_without_pk values (1, 'a', 'b');
insert into table_without_pk values (2, 'a', 'b');
update table_without_pk set a = 'A' where i = 2;
update table_without_pk set b = 'B' where i = 2;
delete from table_without_pk where i = 1;
SELECT data FROM pg_logical_slot_get_changes('regression_slot', NULL, NULL, 'pretty-print', '1');

-- That's fine: let's do replica
alter table table_without_pk replica identity full;

-- Schema repeated on new plugin chunk
insert into table_without_pk values (3, 'a', 'b');
insert into table_without_pk values (4, 'a', 'b');
update table_without_pk set a = 'A' where i = 4;
update table_without_pk set b = 'B' where i = 4;
delete from table_without_pk where i = 3;
SELECT data FROM pg_logical_slot_get_changes('regression_slot', NULL, NULL, 'pretty-print', '1');

-- Schema change detected before new chunk
alter table table_without_pk add c text;
insert into table_without_pk values (5, 'a', 'b', 'c');
insert into table_without_pk values (6, 'a', 'b', 'c');
SELECT data FROM pg_logical_slot_get_changes('regression_slot', NULL, NULL, 'pretty-print', '1');

-- Schema change detected within a new chunk
insert into table_without_pk values (7, 'a', 'b', 'c');
insert into table_without_pk values (8, 'a', 'b', 'c');
alter table table_without_pk add d text;
insert into table_without_pk values (9, 'a', 'b', 'c', 'd');
insert into table_without_pk values (10, 'a', 'b', 'c', 'd');
SELECT data FROM pg_logical_slot_get_changes('regression_slot', NULL, NULL, 'pretty-print', '1');

-- Schema change detected within a transaction
begin;
insert into table_without_pk values (11, 'a', 'b', 'c', 'd');
insert into table_without_pk values (12, 'a', 'b', 'c', 'd');
alter table table_without_pk drop d;
insert into table_without_pk values (13, 'a', 'b', 'c');
insert into table_without_pk values (14, 'a', 'b', 'c');
commit;
SELECT data FROM pg_logical_slot_get_changes('regression_slot', NULL, NULL, 'pretty-print', '1');


SELECT 'stop' FROM pg_drop_replication_slot('regression_slot');
