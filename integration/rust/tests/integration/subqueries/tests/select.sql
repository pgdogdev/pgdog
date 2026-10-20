-- case
-- <key> IN (<sq>) where <sq> selects a **non-key** column of another sharded table
SELECT * FROM subusers WHERE id IN (
    SELECT id FROM suborders
) ORDER BY id;

-- case
-- what: .. WHERE <col> IN (<sq>), neither side has a key
SELECT * FROM suborders WHERE id IN (
    SELECT id FROM subusers WHERE status = 'VIP'
) ORDER BY id;

-- case
-- what: SELECT .. FROM <st1> WHERE <col> IN (<sq> WHERE <key> = <k1>)
SELECT * FROM subusers WHERE status IN (
    SELECT status FROM subusers WHERE id = 3
) ORDER BY id;

-- case
-- what: .. WHERE <col> IN (<sq> WHERE <key> = $1)
-- $1 = 3
SELECT * FROM subusers WHERE status IN (
    SELECT status FROM subusers WHERE id = $1
) ORDER BY id;

-- case
-- what: .. WHERE <col> IN (<sq> WHERE <col> = $1)
-- $1 = 'VIP'
SELECT * FROM suborders WHERE id IN (
    SELECT id FROM subusers WHERE status = $1
) ORDER BY id;

-- case
-- what: .. WHERE NOT EXISTS (<sq>)
SELECT * FROM suborders WHERE NOT EXISTS (
    SELECT 1 FROM subusers WHERE id = 3
) ORDER BY id;

-- case
-- what: .. WHERE <col> NOT IN (<sq>), <sq> result with NULLs
SELECT * FROM suborders WHERE state NOT IN (
    SELECT status FROM subusers
) ORDER BY id;

-- case
-- what: .. WHERE <col> NOT IN (<sq>), <sq> result without NULLs
SELECT * FROM suborders WHERE state NOT IN (
    SELECT status FROM subusers WHERE status IS NOT NULL
) ORDER BY id;

-- case
-- what: .. WHERE <col> IN (WITH c AS (..) SELECT .. FROM c)
SELECT * FROM suborders WHERE id IN (
    WITH c AS (SELECT id FROM subusers WHERE status = 'VIP') SELECT id FROM c
) ORDER BY id;
