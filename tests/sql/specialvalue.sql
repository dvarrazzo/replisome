\set VERBOSITY terse
\pset format unaligned

-- predictability
SET synchronous_commit = on;

DROP TABLE IF EXISTS xpto;

SELECT slot_create();

CREATE TABLE xpto (a SERIAL PRIMARY KEY, b bool, c varchar(60), d real);
COMMIT;

BEGIN;
INSERT INTO xpto (b, c, d) VALUES('t', 'test1', '+inf');
INSERT INTO xpto (b, c, d) VALUES('f', 'test2', 'nan');
INSERT INTO xpto (b, c, d) VALUES(NULL, 'null', '-inf');
INSERT INTO xpto (b, c, d) VALUES(TRUE, E'valid: '' " \\ / \b \f \n \r \t \u207F \u967F invalid: \\g \\k end', 123.456);
COMMIT;

SELECT data FROM slot_get();
SELECT slot_drop();
