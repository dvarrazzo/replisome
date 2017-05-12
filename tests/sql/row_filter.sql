\set VERBOSITY terse
\pset format unaligned

-- predictability
SET synchronous_commit = on;

DROP TABLE IF EXISTS rf1;

CREATE TABLE rf1 (id int PRIMARY KEY, data text, d date);

SELECT slot_create();

-- Bad config
SELECT data FROM slot_get(
	'include', '{"where": "id > 0"}');
SELECT data FROM slot_get(
	'exclude', '{"table": "rf1", "where": "id > 0"}');
SELECT data FROM slot_get(
	'include', '{"table": "rf1", "where": "id >"}');

-- Good config
SELECT data FROM slot_get(
	'include', '{"table": "rf1", "where": "whateva"}');
SELECT data FROM slot_get(
	'include', '{"table": "rf1", "where": "id > 0"}');

-- Basic filtering
INSERT INTO rf1 VALUES (1, 'foo', NULL);
INSERT INTO rf1 VALUES (2, 'bar', '2017-01-01');
INSERT INTO rf1 VALUES (3, 'baz', '2017-02-01');

SELECT data FROM slot_peek(
	'include', '{"table": "rf1", "where": "nofield = 42"}');
SELECT data FROM slot_peek(
	'include', '{"table": "rf1", "where": "id % 2 = 1"}');
SELECT data FROM slot_peek(
	'include', '{"table": "rf1", "where": "d is not null"}');
SELECT data FROM slot_peek(
	'include',
	'{"table": "rf1", "where": "date_trunc(''month'', d) > ''2017-01-15''::date"}');
SELECT data FROM slot_get(
	'include', '{"table": "rf1", "where": "data ~ ''^ba.''"}');

SELECT slot_drop();
