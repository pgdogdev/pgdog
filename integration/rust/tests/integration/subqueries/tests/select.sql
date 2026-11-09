-- case
-- <key> IN (<sq>) where <sq> selects a **non-key** column of another sharded table
-- route select: 0,1
-- route select: 0,1
SELECT * FROM subusers WHERE id IN (
    SELECT id FROM suborders
) ORDER BY id;

-- case
-- what: .. WHERE <col> IN (<sq>), neither side has a key
-- route select: 0,1
-- route select: 0,1
SELECT * FROM suborders WHERE id IN (
    SELECT id FROM subusers WHERE status = 'VIP'
) ORDER BY id;

-- case
-- what: SELECT .. FROM <st1> WHERE <col> IN (<sq> WHERE <key> = <k1>)
-- route select: 1
-- route select: 0,1
SELECT * FROM subusers WHERE status IN (
    SELECT status FROM subusers WHERE id = 3
) ORDER BY id;

-- case
-- what: .. WHERE <col> IN (<sq> WHERE <key> = $1)
-- route select: 1
-- route select: 0,1
-- $1 = 3
SELECT * FROM subusers WHERE status IN (
    SELECT status FROM subusers WHERE id = $1
) ORDER BY id;

-- case
-- what: .. WHERE <col> IN (<sq> WHERE <col> = $1)
-- route select: 0,1
-- route select: 0,1
-- $1 = 'VIP'
SELECT * FROM suborders WHERE id IN (
    SELECT id FROM subusers WHERE status = $1
) ORDER BY id;

-- case
-- what: .. WHERE NOT EXISTS (<sq>)
-- route select: 1
-- route select: 0,1
SELECT * FROM suborders WHERE NOT EXISTS (
    SELECT 1 FROM subusers WHERE id = 3
) ORDER BY id;

-- case
-- what: .. WHERE <col> NOT IN (<sq>), <sq> result with NULLs
-- route select: 0,1
-- route select: 0,1
SELECT * FROM suborders WHERE state NOT IN (
    SELECT status FROM subusers
) ORDER BY id;

-- case
-- what: .. WHERE <col> NOT IN (<sq>), <sq> result without NULLs
-- route select: 0,1
-- route select: 0,1
SELECT * FROM suborders WHERE state NOT IN (
    SELECT status FROM subusers WHERE status IS NOT NULL
) ORDER BY id;

-- case
-- what: .. WHERE <col> IN (WITH c AS (..) SELECT .. FROM c)
-- route select: 0,1
-- route select: 0,1
SELECT * FROM suborders WHERE id IN (
    WITH c AS (SELECT id FROM subusers WHERE status = 'VIP') SELECT id FROM c
) ORDER BY id;

-- case
-- what: .. (SELECT <agg>(<col>) FROM <st2>), <agg> = count
-- route select: 0,1
-- route select: 0,1
SELECT * FROM suborders WHERE id = (
    SELECT count(status) FROM subusers
) ORDER BY id;

-- case
-- what: .. (SELECT <agg>(<col>) FROM <st2>), <agg> = sum
-- route select: 0,1
-- route select: 0,1
SELECT * FROM suborders WHERE id + 1 = (
    SELECT sum(id) FROM subusers WHERE status = 'VIP'
) ORDER BY id;

-- case
-- what: .. (SELECT <agg>(<col>) FROM <st2>), <agg> = min
-- route select: 0,1
-- route select: 0,1
SELECT * FROM suborders WHERE id = (
    SELECT min(id) FROM subusers WHERE id > 1
) ORDER BY id;

-- case
-- what: .. (SELECT <agg>(<col>) FROM <st2>), <agg> = max
-- route select: 0,1
-- route select: 0,1
SELECT * FROM suborders WHERE id = (
    SELECT max(id) FROM subusers WHERE id < 4
) ORDER BY id;

-- case
-- what: .. (SELECT <agg>(<col>) FROM <st2>), <agg> = avg
-- route select: 0,1
-- route select: 0,1
SELECT * FROM suborders WHERE id = (
    SELECT avg(id) FROM subusers WHERE id < 4
) ORDER BY id;

-- case
-- what: .. (SELECT DISTINCT .. FROM <st2>)
-- route select: 0,1
-- route select: 0,1
SELECT * FROM subusers WHERE status = (
    SELECT DISTINCT status FROM subusers WHERE id IN (1, 3)
) ORDER BY id;

-- case
-- what: .. (SELECT .. FROM <st2> ORDER BY .. LIMIT <n>)
-- route select: 0,1
-- route select: 0,1
SELECT * FROM suborders WHERE id IN (
    SELECT id FROM subusers ORDER BY id DESC LIMIT 3
) ORDER BY id;

-- case
-- what: SELECT .. FROM <st1> WHERE <key> = <k1> AND <col> IN (<sq>), <sq> has no key
-- route select: 0,1
-- route select: 1
SELECT * FROM subusers WHERE id = 3 AND status IN (
    SELECT status FROM subusers WHERE username = 'test'
) ORDER BY id;

-- case
-- what: SELECT .. FROM <st1> WHERE <key> = <k1> AND <col> IN (<sq> WHERE <key> = <k1>), same shard
-- route select: 1
SELECT * FROM subusers WHERE id = 3 AND status IN (
    SELECT status FROM subusers WHERE id = 3
) ORDER BY id;

-- case
-- what: .. WHERE <key> = $1 AND <col> IN (<sq> WHERE <key> = $1), same parameter in outer and inner
-- route select: 1
-- route select: 1
-- $1 = 3
SELECT * FROM subusers WHERE id = $1 AND status IN (
    SELECT status FROM subusers WHERE id = $1
) ORDER BY id;

-- case
-- what: SELECT <agg>(..) FROM (<sq>) s, <sq> is cross-shard
-- route select: 0,1
-- route select: any
SELECT count(*) FROM (
    SELECT id FROM subusers WHERE status = 'VIP'
) s;

-- case
-- what: SELECT .. FROM (SELECT .. ORDER BY .. LIMIT <n>) s, the <n> rows come from several shards
-- route select: 0,1
-- route select: any
SELECT s.id, s.username FROM (
    SELECT id, username FROM subusers ORDER BY id DESC LIMIT 3
) s ORDER BY s.id;

-- case
-- what: SELECT .. FROM (<sq>) s ORDER BY .. LIMIT <n>, <sq> is cross-shard
-- route select: 0,1
-- route select: any
SELECT s.id, s.status FROM (
    SELECT id, status FROM subusers WHERE status IS NOT NULL
) s ORDER BY s.id DESC LIMIT 2;

-- case
-- what: Derived table with an inner-table that returns an empty response.
--       Want to make sure that the OIDs returned are correct (if present)
--       based on us relying on the first row normally setting them
--       properly in the response via explicit casts.
-- route select: 0,1
-- route select: any
SELECT s.id, s.status FROM (
    SELECT id, status FROM subusers WHERE status = 'some random status which doesnt exist'
) s ORDER BY s.id DESC LIMIT 2;
