\set aid random(1, 100000)
\set bid random(1, 1000)
\set tid random(1, 10)
\set delta random(-5000, 5000)

UPDATE pgbench_accounts
SET abalance = abalance + :delta
WHERE aid = :aid;

SELECT abalance
FROM pgbench_accounts
WHERE aid = :aid;

BEGIN;
UPDATE pgbench_tellers
SET tbalance = tbalance + :delta
WHERE tid = :tid;
COMMIT;

UPDATE pgbench_branches
SET bbalance = bbalance + :delta
WHERE bid = :bid;

INSERT INTO pgbench_history (tid, bid, aid, delta, mtime)
VALUES (:tid, :bid, :aid, :delta, CURRENT_TIMESTAMP);

SELECT abalance
FROM pgbench_accounts
WHERE aid = :aid;

SELECT bbalance
FROM pgbench_branches
WHERE bid = :bid;
