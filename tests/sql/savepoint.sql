\set VERBOSITY terse
\pset format unaligned

-- predictability
SET synchronous_commit = on;

CREATE TABLE xpto (a SERIAL PRIMARY KEY, b text);

SELECT slot_create();

INSERT INTO xpto (b) VALUES('john');
INSERT INTO xpto (b) VALUES('smith');
INSERT INTO xpto (b) VALUES('robert');

BEGIN;
INSERT INTO xpto (b) VALUES('marie');
SAVEPOINT sp1;
INSERT INTO xpto (b) VALUES('ernesto');
SAVEPOINT sp2;
INSERT INTO xpto (b) VALUES('peter');	-- discard
SAVEPOINT sp3;
INSERT INTO xpto (b) VALUES('albert');	-- discard
ROLLBACK TO SAVEPOINT sp2;
RELEASE SAVEPOINT sp1;
INSERT INTO xpto (b) VALUES('francisco');
END;

SELECT data FROM slot_get();
SELECT slot_drop();
