-- case
-- what: INSERT INTO <st1> SELECT .. FROM <st2>, no sharding key value anywhere in the INSERT statement
-- route select: 0,1
-- route insert: 0
-- verify: SELECT * FROM subpackages ORDER BY id
-- name: test
INSERT INTO subpackages (id, user_id, order_id) SELECT id, user_id, id FROM suborders

-- case
-- what: INSERT INTO <st1> SELECT .. FROM <st2>, source rows are on other shards than their destination
-- route select: 0,1
-- route insert: 1
-- verify: SELECT * FROM subpackages ORDER BY id
INSERT INTO subpackages (id, user_id, order_id) SELECT id, user_id + 2, NULL FROM suborders

-- case
-- what: INSERT INTO <st1> SELECT .. RETURNING ..
-- route select: 0,1 
-- route insert: 0,1
-- verify: SELECT * FROM subpackages ORDER BY id
-- name: test2
INSERT INTO subpackages (id, user_id, order_id) SELECT id, id + 1, NULL FROM suborders RETURNING *

-- case
-- what: INSERT INTO <st1> SELECT .. FROM <st1>
-- verify: SELECT * FROM subusers ORDER BY id
-- route select: 0,1
-- route insert: 0,1
-- name: subusers_test
INSERT INTO subusers (id, username) SELECT id * 10, username FROM subusers

-- case
-- name: current
-- what: INSERT INTO <st1> SELECT .. WHERE <col> IN (<sq> WHERE <x> = $1), <x> is <col>
-- $1 = VIP
-- route select: 0,1
-- route select: 0,1
-- route insert: 0
-- verify: SELECT * FROM subpackages ORDER BY id
INSERT INTO subpackages (id, user_id, order_id) SELECT id, user_id, id FROM suborders WHERE user_id IN (SELECT id FROM subusers WHERE status = $1::text)

-- case
-- name: current2
-- what: INSERT INTO <st1> SELECT .. WHERE <col> IN (<sq> WHERE <x> = $1), <x> is <key>
-- $1 = 1
-- route select: 0
-- route select: 0
-- route insert: 0
-- verify: SELECT * FROM subpackages ORDER BY id
INSERT INTO subpackages (id, user_id, order_id) SELECT id, user_id, id FROM suborders WHERE user_id IN (SELECT id FROM subusers WHERE id = $1)

-- case
-- what: INSERT INTO <st1> SELECT DISTINCT .. FROM <st2>
-- route select: 0,1
-- route insert: 0,1
-- verify: SELECT * FROM substates ORDER BY state
INSERT INTO substates (state, shipping) SELECT DISTINCT status, 'distinct' FROM subusers WHERE status IS NOT NULL

-- case
-- what: INSERT INTO <st1> SELECT .. ON CONFLICT DO NOTHING
-- route select: 0,1
-- route insert: 0,1
-- verify: SELECT * FROM subusers ORDER BY id
INSERT INTO subusers (id, username, status) SELECT id * 4, 'conflict', state FROM suborders ON CONFLICT DO NOTHING

-- case
-- what: INSERT INTO <st1> SELECT .. ON CONFLICT DO UPDATE SET <col> = EXCLUDED.<col>
-- route select: 0,1
-- route insert: 0,1
-- verify: SELECT * FROM subusers ORDER BY id
INSERT INTO subusers (id, username, status) SELECT id * 4, 'conflict', state FROM suborders ON CONFLICT (id) DO UPDATE SET status = EXCLUDED.status

-- case
-- what: INSERT INTO <st1> SELECT count(<col>) FROM <st2>
-- route select: 0,1
-- route insert: 1
-- verify: SELECT * FROM subpackages ORDER BY id
INSERT INTO subpackages (id, user_id) SELECT count(*), count(*) FROM suborders

-- case
-- what: INSERT INTO <st1> SELECT sum(<col>) FROM <st2>
-- route select: 0,1
-- route insert: 1
-- verify: SELECT * FROM subpackages ORDER BY id
INSERT INTO subpackages (id, user_id) SELECT sum(id), sum(user_id) FROM suborders

-- case
-- what: INSERT INTO <st1> SELECT min(<col>) FROM <st2>
-- route select: 0,1
-- route insert: 0
-- verify: SELECT * FROM subpackages ORDER BY id
INSERT INTO subpackages (id, user_id) SELECT min(id), min(user_id) FROM suborders

-- case
-- what: INSERT INTO <st1> SELECT max(<col>) FROM <st2>
-- route select: 0,1
-- route insert: 0
-- verify: SELECT * FROM subpackages ORDER BY id
INSERT INTO subpackages (id, user_id) SELECT max(id), max(user_id) FROM suborders

-- case
-- what: INSERT INTO <st1> SELECT avg(<col>) FROM <st2>
-- route select: 0,1
-- route insert: 0
-- verify: SELECT * FROM subpackages ORDER BY id
INSERT INTO subpackages (id, user_id) SELECT avg(id), avg(id) FROM suborders

-- case
-- what: INSERT INTO <st1> SELECT .. FROM generate_series(..)
-- route select: any
-- route insert: 0,1
-- verify: SELECT * FROM subusers ORDER BY id
INSERT INTO subusers(id) SELECT id FROM generate_series(10, 1000) as id

-- case
-- what: INSERT INTO <st1> SELECT .. FROM unnest($1, ..)
-- $1 = {10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25,26,27,28,29,30,31,32,33,34,35,36}
-- route select: any
-- route insert: 0,1
-- verify: SELECT * FROM subusers ORDER BY id
INSERT INTO subusers(id) SELECT id FROM UNNEST($1::int[]) as id

-- case
-- what: INSERT INTO <st1> SELECT .. FROM <st2> ORDER BY .. LIMIT <n>
-- route select: 0,1
-- route insert: 0,1
-- verify: SELECT * FROM subpackages ORDER BY id
INSERT INTO subpackages (id, user_id) SELECT id, id FROM subusers ORDER BY id DESC LIMIT 3
