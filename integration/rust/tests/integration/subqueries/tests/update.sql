-- case
-- what: self-reference .. <st1> .. WHERE <col> IN (SELECT .. FROM <st1>)
-- route select: 0
-- route update: 0,1
-- verify: SELECT * FROM subusers ORDER BY id
UPDATE subusers SET status = 'Not VIP' WHERE username IN (SELECT username FROM subusers WHERE id IN (1, 2))

-- case
-- what: .. WHERE <col> IN (<sq> WHERE <x> = $1), <x> is <col>
-- route select: 0,1
-- route update: 0,1
-- $1 = 'VIP'
-- verify: SELECT * FROM suborders ORDER BY id
UPDATE suborders SET state = 'CA' WHERE id IN (SELECT id FROM subusers WHERE status = $1)

-- case
-- what: .. WHERE <col> IN (<sq> WHERE <x> = $1), <x> is <key>
-- route select: 1
-- route update: 0,1
-- $1 = 3
-- verify: SELECT * FROM suborders ORDER BY id
UPDATE suborders SET state = 'CA' WHERE id IN (SELECT id FROM subusers WHERE id = $1)

-- case
-- what: .. WHERE <col> <op> (<sq>), <op> = IN
-- route select: 0,1
-- route update: 0,1
-- verify: SELECT * FROM suborders ORDER BY id
UPDATE suborders SET state = 'CA' WHERE id IN (SELECT id FROM subusers)

-- case
-- what: .. WHERE <col> <op> (<sq>), <op> = NOT IN (with NULLs)
-- route select: 0,1
-- route update: 0,1
-- verify: SELECT * FROM suborders ORDER BY id
UPDATE suborders SET state = 'CA' WHERE state NOT IN (SELECT status FROM subusers)

-- case
-- what: .. WHERE <op> (<sq>), <op> = EXISTS
-- route select: 1
-- route update: 0,1
-- verify: SELECT * FROM suborders ORDER BY id
UPDATE suborders SET state = 'CA' WHERE EXISTS (SELECT 1 FROM subusers WHERE id = 3)

-- case
-- what: .. WHERE <op> (<sq>), <op> = NOT EXISTS
-- route select: 1
-- route update: 0,1
-- verify: SELECT * FROM suborders ORDER BY id
UPDATE suborders SET state = 'CA' WHERE NOT EXISTS (SELECT 1 FROM subusers WHERE id = 3)

-- case
-- what: .. WHERE <col> <op> (<sq>), <op> = scalar comparison
-- route select: 0,1
-- route update: 0,1
-- verify: SELECT * FROM suborders ORDER BY id
UPDATE suborders SET state = 'CA' WHERE id = (SELECT max(id) FROM subusers WHERE status = 'VIP')

-- case
-- what: .. WHERE <key> IN (<sq>)
-- route select: 0,1
-- route update: 1
-- verify: SELECT * FROM subusers ORDER BY id
UPDATE subusers SET status = 'changed' WHERE id IN (SELECT id FROM suborders WHERE state = 'NC')

-- case
-- what: .. (SELECT .. FROM <st2> ORDER BY .. LIMIT <n>)
-- route select: 0,1
-- route update: 0,1
-- verify: SELECT * FROM suborders ORDER BY id
UPDATE suborders SET state = 'NC' WHERE id IN (SELECT id FROM subusers ORDER BY id DESC LIMIT 3)

-- case
-- what: .. WHERE <key> = <k1> AND <col> IN (<sq>), key in outer only
-- route select: 0,1
-- route update: 1
-- verify: SELECT * FROM subusers ORDER BY id
UPDATE subusers SET status = 'changed' WHERE id = 3 AND status IN (SELECT status FROM subusers WHERE username = 'test')

-- case
-- what: .. WHERE <key> = <k1> AND <col> IN (<sq>), key in both, same shard
-- route select: 1
-- verify: SELECT * FROM subusers ORDER BY id
UPDATE subusers SET status = 'changed' WHERE id = 3 AND status IN (SELECT status FROM subusers WHERE id = 3)

-- case
-- what: .. WHERE <col> IN (<sq> WHERE <key> = <k1>)
-- route select: 1
-- route update: 0,1
-- verify: SELECT * FROM subusers ORDER BY id
UPDATE subusers SET status = 'changed' WHERE status IN (SELECT status FROM subusers WHERE id = 3)

-- case
-- TODO(jkaczman): This case is not handled yet, as noted by the `ShardingKeyUpdate` TODO
-- present in the file (parser/rewrite/statement/subquery/from_clause.rs)
-- marked TODO
-- what: ShardingKeyUpdate pulling data from non-sharded values (generate_series method)
-- route select: any
-- route update: 0,1
-- verify: SELECT * FROM suborders ORDER BY id
UPDATE suborders SET user_id = s.new_user_id FROM (SELECT val AS old_user_id, val + 2 AS new_user_id FROM generate_series(1, 2) AS g(val)) s WHERE suborders.user_id = s.old_user_id

-- case
-- what: UPDATE <st1> SET .. FROM (<sq>) s WHERE ..
-- route select: 0,1
-- route update: 0,1
-- verify: SELECT * FROM suborders ORDER BY id
UPDATE suborders SET state = 'NC' FROM (SELECT id FROM subusers WHERE status = 'VIP') s WHERE suborders.id = s.id

-- case
-- what: UPDATE <st1> SET .. FROM <st2> WHERE <st1>.<x> = <st2>.<x>, <x> is <key> (co-located)
-- route update: 0,1
-- verify: SELECT * FROM suborders ORDER BY id
UPDATE suborders SET state = 'NC' FROM subusers WHERE suborders.user_id = subusers.id AND subusers.status = 'VIP'
