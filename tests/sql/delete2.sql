\set VERBOSITY terse
\pset format unaligned

-- predictability
SET synchronous_commit = on;

DROP TABLE IF EXISTS table_with_pk;
DROP TABLE IF EXISTS table_without_pk;
DROP TABLE IF EXISTS table_with_unique;

CREATE TABLE table_with_pk (
a	smallserial,
b	smallint,
c	int,
d	bigint,
e	numeric(5,3),
f	real not null,
g	double precision,
h	char(10),
i	varchar(30),
j	text,
k	bit varying(20),
l	timestamp,
m	date,
n	boolean not null,
o	json,
p	tsvector,
PRIMARY KEY(b, c, d)
);

CREATE TABLE table_without_pk (
a	smallserial,
b	smallint,
c	int,
d	bigint,
e	numeric(5,3),
f	real not null,
g	double precision,
h	char(10),
i	varchar(30),
j	text,
k	bit varying(20),
l	timestamp,
m	date,
n	boolean not null,
o	json,
p	tsvector
);

CREATE TABLE table_with_unique (
a	smallserial,
b	smallint,
c	int,
d	bigint,
e	numeric(5,3) not null,
f	real not null,
g	double precision not null,
h	char(10),
i	varchar(30),
j	text,
k	bit varying(20),
l	timestamp,
m	date,
n	boolean not null,
o	json,
p	tsvector,
UNIQUE(g, n)
);

-- INSERT
INSERT INTO table_with_pk (b, c, d, e, f, g, h, i, j, k, l, m, n, o, p) VALUES(1, 2, 3, 3.54, 876.563452345, 1.23, 'teste', 'testando', 'um texto longo', B'001110010101010', '2013-11-02 17:30:52', '2013-02-04', true, '{ "a": 123 }', 'Old Old Parr'::tsvector);
INSERT INTO table_without_pk (b, c, d, e, f, g, h, i, j, k, l, m, n, o, p) VALUES(1, 2, 3, 3.54, 876.563452345, 1.23, 'teste', 'testando', 'um texto longo', B'001110010101010', '2013-11-02 17:30:52', '2013-02-04', true, '{ "a": 123 }', 'Old Old Parr'::tsvector);
INSERT INTO table_with_unique (b, c, d, e, f, g, h, i, j, k, l, m, n, o, p) VALUES(1, 2, 3, 3.54, 876.563452345, 1.23, 'teste', 'testando', 'um texto longo', B'001110010101010', '2013-11-02 17:30:52', '2013-02-04', true, '{ "a": 123 }', 'Old Old Parr'::tsvector);

SELECT slot_create();

-- DELETE: REPLICA IDENTITY NOTHING
ALTER TABLE table_with_pk REPLICA IDENTITY NOTHING;
DELETE FROM table_with_pk WHERE b = 1;
ALTER TABLE table_with_pk REPLICA IDENTITY DEFAULT;

ALTER TABLE table_without_pk REPLICA IDENTITY NOTHING;
DELETE FROM table_without_pk WHERE b = 1;
ALTER TABLE table_without_pk REPLICA IDENTITY DEFAULT;

ALTER TABLE table_with_unique REPLICA IDENTITY NOTHING;
DELETE FROM table_with_unique WHERE b = 1;
ALTER TABLE table_with_unique REPLICA IDENTITY DEFAULT;

SELECT data FROM slot_get('include-empty-xacts', '1');

-- Test skipping empty xacts
ALTER TABLE table_with_pk REPLICA IDENTITY NOTHING;
DELETE FROM table_with_pk WHERE b = 1;
ALTER TABLE table_with_pk REPLICA IDENTITY DEFAULT;

ALTER TABLE table_without_pk REPLICA IDENTITY NOTHING;
DELETE FROM table_without_pk WHERE b = 1;
ALTER TABLE table_without_pk REPLICA IDENTITY DEFAULT;

ALTER TABLE table_with_unique REPLICA IDENTITY NOTHING;
DELETE FROM table_with_unique WHERE b = 1;
ALTER TABLE table_with_unique REPLICA IDENTITY DEFAULT;

SELECT data FROM slot_get();

SELECT slot_drop();
