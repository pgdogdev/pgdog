-- case
-- what: self-reference .. <st1> .. WHERE <col> IN (SELECT .. FROM <st1>)
-- verify: SELECT * FROM subusers ORDER BY id
-- setup: reset tables with fresh data
UPDATE subusers SET status = 'Not VIP' WHERE username IN (SELECT username FROM subusers WHERE id IN (1, 2))

-- case
-- what: .. WHERE <col> IN (<sq> WHERE <x> = $1), <x> is <col>
-- $1 = 'VIP'
-- verify: SELECT * FROM suborders ORDER BY id
-- setup: reset tables with fresh data
UPDATE suborders SET state = 'CA' WHERE id IN (SELECT id FROM subusers WHERE status = $1)

-- case
-- what: .. WHERE <col> IN (<sq> WHERE <x> = $1), <x> is <key>
-- $1 = 3
-- verify: SELECT * FROM suborders ORDER BY id
-- setup: reset tables with fresh data
UPDATE suborders SET state = 'CA' WHERE id IN (SELECT id FROM subusers WHERE id = $1)

-- case
-- what: .. WHERE <col> <op> (<sq>), <op> = IN
-- verify: SELECT * FROM suborders ORDER BY id
-- setup: reset tables with fresh data
UPDATE suborders SET state = 'CA' WHERE id IN (SELECT id FROM subusers)

-- case
-- what: .. WHERE <col> <op> (<sq>), <op> = NOT IN (with NULLs)
-- verify: SELECT * FROM suborders ORDER BY id
-- setup: reset tables with fresh data
UPDATE suborders SET state = 'CA' WHERE state NOT IN (SELECT status FROM subusers)

-- case
-- what: .. WHERE <op> (<sq>), <op> = EXISTS
-- verify: SELECT * FROM suborders ORDER BY id
-- setup: reset tables with fresh data
UPDATE suborders SET state = 'CA' WHERE EXISTS (SELECT 1 FROM subusers WHERE id = 3)

-- case
-- what: .. WHERE <op> (<sq>), <op> = NOT EXISTS
-- verify: SELECT * FROM suborders ORDER BY id
-- setup: reset tables with fresh data
UPDATE suborders SET state = 'CA' WHERE NOT EXISTS (SELECT 1 FROM subusers WHERE id = 3)

-- case
-- what: .. WHERE <col> <op> (<sq>), <op> = scalar comparison
-- verify: SELECT * FROM suborders ORDER BY id
-- setup: reset tables with fresh data
UPDATE suborders SET state = 'CA' WHERE id = (SELECT max(id) FROM subusers WHERE status = 'VIP')

-- case
-- what: .. WHERE <key> IN (<sq>)
-- verify: SELECT * FROM subusers ORDER BY id
-- setup: reset tables with fresh data
UPDATE subusers SET status = 'changed' WHERE id IN (SELECT id FROM suborders WHERE state = 'NC')
