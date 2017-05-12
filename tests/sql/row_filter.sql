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


SELECT slot_drop();
