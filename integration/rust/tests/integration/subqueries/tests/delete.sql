-- case
-- what: self-reference .. <st1> .. WHERE <col> IN (SELECT .. FROM <st1>)
-- route select: 0
-- route delete: 0,1
-- verify: SELECT * FROM suborders ORDER BY id
DELETE FROM suborders WHERE state IN (SELECT state FROM suborders WHERE user_id = 2)

-- case
-- what: .. WHERE <col> IN (<sq> WHERE <x> = $1), <x> is <col>
-- route select: 0,1
-- route delete: 0,1
-- $1 = 'VIP'
-- verify: SELECT * FROM suborders ORDER BY id
DELETE FROM suborders WHERE id IN (SELECT id FROM subusers WHERE status = $1)

-- case
-- what: .. WHERE <col> IN (<sq> WHERE <x> = $1), <x> is <key>
-- route select: 1
-- route delete: 0,1
-- $1 = 3
-- verify: SELECT * FROM suborders ORDER BY id
DELETE FROM suborders WHERE id IN (SELECT id FROM subusers WHERE id = $1)

-- case
-- what: .. WHERE <col> <op> (<sq>), <op> = IN
-- route select: 0,1
-- route delete: 0,1
-- verify: SELECT * FROM suborders ORDER BY id
DELETE FROM suborders WHERE id IN (SELECT id FROM subusers)

-- case
-- what: .. WHERE <col> <op> (<sq>), <op> = NOT IN (with NULLs)
-- route select: 0,1
-- route delete: 0,1
-- verify: SELECT * FROM suborders ORDER BY id
DELETE FROM suborders WHERE state NOT IN (SELECT status FROM subusers)

-- case
-- what: .. WHERE <op> (<sq>), <op> = EXISTS
-- route select: 1
-- route delete: 0,1
-- verify: SELECT * FROM suborders ORDER BY id
DELETE FROM suborders WHERE EXISTS (SELECT 1 FROM subusers WHERE id = 3)

-- case
-- what: .. WHERE <op> (<sq>), <op> = NOT EXISTS
-- route select: 1
-- route delete: 0,1
-- verify: SELECT * FROM suborders ORDER BY id
DELETE FROM suborders WHERE NOT EXISTS (SELECT 1 FROM subusers WHERE id = 3)

-- case
-- what: .. WHERE <col> <op> (<sq>), <op> = scalar comparison
-- route select: 0,1
-- route delete: 0,1
-- verify: SELECT * FROM suborders ORDER BY id
DELETE FROM suborders WHERE id = (SELECT max(id) FROM subusers WHERE status = 'VIP')

-- case
-- what: .. WHERE <key> IN (<sq>)
-- route select: 0,1
-- route delete: 1
-- verify: SELECT * FROM subusers ORDER BY id
DELETE FROM subusers WHERE id IN (SELECT id FROM suborders WHERE state = 'NC')

-- case
-- what: .. (SELECT <agg>(<col>) FROM <st2>), <agg> = count
-- route select: 0,1
-- route delete: 0,1
-- verify: SELECT * FROM suborders ORDER BY id
DELETE FROM suborders WHERE id = (SELECT count(status) FROM subusers)

-- case
-- what: .. (SELECT <agg>(<col>) FROM <st2>), <agg> = sum
-- route select: 0,1
-- route delete: 0,1
-- verify: SELECT * FROM suborders ORDER BY id
DELETE FROM suborders WHERE id + 1 = (SELECT sum(id) FROM subusers WHERE status = 'VIP')

-- case
-- what: .. (SELECT <agg>(<col>) FROM <st2>), <agg> = min
-- route select: 0,1
-- route delete: 0,1
-- verify: SELECT * FROM suborders ORDER BY id
DELETE FROM suborders WHERE id = (SELECT min(id) FROM subusers WHERE id > 1)

-- case
-- what: .. (SELECT <agg>(<col>) FROM <st2>), <agg> = max
-- route select: 0,1
-- route delete: 0,1
-- verify: SELECT * FROM suborders ORDER BY id
DELETE FROM suborders WHERE id = (SELECT max(id) FROM subusers WHERE id < 4)

-- case
-- what: .. (SELECT <agg>(<col>) FROM <st2>), <agg> = avg
-- route select: 0,1
-- route delete: 0,1
-- verify: SELECT * FROM suborders ORDER BY id
DELETE FROM suborders WHERE id = (SELECT avg(id) FROM subusers WHERE id < 4)

-- case
-- what: .. WHERE <key> = <k1> AND <col> IN (<sq>), key in outer only
-- route select: 0,1
-- route delete: 1
-- verify: SELECT * FROM subusers ORDER BY id
DELETE FROM subusers WHERE id = 3 AND status IN (SELECT status FROM subusers WHERE username = 'test')

-- case
-- what: .. WHERE <key> = <k1> AND <col> IN (<sq>), key in both, same shard
-- route select: 1
-- verify: SELECT * FROM subusers ORDER BY id
DELETE FROM subusers WHERE id = 3 AND status IN (SELECT status FROM subusers WHERE id = 3)

-- case
-- what: .. WHERE <col> IN (<sq> WHERE <key> = <k1>)
-- route select: 1
-- route delete: 0,1
-- verify: SELECT * FROM subusers ORDER BY id
DELETE FROM subusers WHERE username IN (SELECT username FROM subusers WHERE id = 3)
