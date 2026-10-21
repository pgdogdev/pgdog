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
