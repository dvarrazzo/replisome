\set VERBOSITY terse
\pset format unaligned

-- predictability
SET synchronous_commit = on;

DROP TABLE IF EXISTS xpto;

SELECT setseed(0);
CREATE TABLE xpto (
id			serial primary key,
rand1		float8 DEFAULT random(),
bincol		bytea
);

SELECT slot_create();

INSERT INTO xpto (bincol) SELECT decode(string_agg(to_char(round(g.i * random()), 'FM0000'), ''), 'hex') FROM generate_series(500, 5000) g(i);
UPDATE xpto SET rand1 = 123.456 WHERE id = 1;
DELETE FROM xpto WHERE id = 1;

SELECT data FROM slot_get();
SELECT slot_drop();
