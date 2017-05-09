\set VERBOSITY terse
\pset format unaligned

-- predictability
SET synchronous_commit = on;

DROP TABLE IF EXISTS xpto;

SELECT setseed(0);
CREATE TABLE xpto (
id				serial primary key,
toasted_col1	text,
rand1			float8 DEFAULT random(),
toasted_col2	text,
rand2 float8	DEFAULT random()
);

SELECT slot_create();

-- uncompressed external toast data
INSERT INTO xpto (toasted_col1, toasted_col2) SELECT string_agg(g.i::text, ''), string_agg((g.i*2)::text, '') FROM generate_series(1, 2000) g(i);

-- compressed external toast data
INSERT INTO xpto (toasted_col2) SELECT repeat(string_agg(to_char(g.i, 'FM0000'), ''), 50) FROM generate_series(1, 500) g(i);

-- update of existing column
UPDATE xpto SET toasted_col1 = (SELECT string_agg(g.i::text, '') FROM generate_series(1, 2000) g(i)) WHERE id = 1;

UPDATE xpto SET rand1 = 123.456 WHERE id = 1;

DELETE FROM xpto WHERE id = 1;

SELECT data FROM slot_get();
SELECT slot_drop();
