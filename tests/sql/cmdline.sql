\set VERBOSITY terse
\pset format unaligned

-- predictability
SET synchronous_commit = on;

SELECT slot_create();

-- Unknown option
SELECT data FROM slot_get('nosuchopt', '42');

-- Bad include
SELECT data FROM slot_get('include', '');
SELECT data FROM slot_get('include', '{');
SELECT data FROM slot_get('include', 'null');
SELECT data FROM slot_get('include', '[]');
SELECT data FROM slot_get('include', '"ciao"');
SELECT data FROM slot_get('include', '{}');
SELECT data FROM slot_get('include', '{"table": "a", "tables": "a"}');

-- Regexp error
SELECT data FROM slot_get('include', '{"tables": "("}');

-- Bad exclude
SELECT data FROM slot_get('exclude', '[]');
SELECT data FROM slot_get('exclude', '{}');
SELECT data FROM slot_get('exclude', '{"table": "a", "columns": ["a"]}');

-- By default don't write in chunks
CREATE TABLE x ();
DROP TABLE x;
SELECT data FROM slot_peek('include-empty-xacts', '1', 'pretty-print', '0');
SELECT data FROM slot_get('write-in-chunks', 't', 'include-empty-xacts', '1', 'pretty-print', '0');

-- By default don't write xids
CREATE TABLE gimmexid (id integer PRIMARY KEY);
INSERT INTO gimmexid values (1);
DROP TABLE gimmexid;
SELECT max(((data::json) -> 'xid')::text::int) < txid_current()
	FROM slot_peek('include-xids', '1', 'include-empty-xacts', '1');
SELECT max(((data::json) -> 'xid')::text::int) + 10 > txid_current()
	FROM slot_peek( 'include-xids', '1', 'include-empty-xacts', '1');
SELECT data FROM slot_get('include-empty-xacts', '1')
	WHERE ((data::json) -> 'xid') IS NOT NULL;

-- By default don't include empty transactions
CREATE TABLE emptyxact (id integer PRIMARY KEY);
INSERT INTO emptyxact values (1);
DROP TABLE emptyxact;
SELECT data FROM slot_get('pretty-print', '0');

SELECT slot_drop();
