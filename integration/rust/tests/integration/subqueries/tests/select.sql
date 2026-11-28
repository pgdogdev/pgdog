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
-- sql: SELECT id FROM subusers WHERE status = 'VIP'
-- route select: 0,1
-- sql: SELECT * FROM suborders WHERE id IN (SELECT u.c1 FROM unnest($1::pg_catalog._int8) u(c1)) ORDER BY id
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
-- sql: SELECT 1 FROM subusers WHERE id = 3
-- route select: 0,1
-- sql: SELECT * FROM suborders WHERE NOT true ORDER BY id
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
-- what: .. WHERE <col> NOT IN (<sq>), <sq> returns zero rows
-- route select: 0,1
-- route select: 0,1
SELECT * FROM suborders WHERE state NOT IN (
    SELECT status FROM subusers WHERE status = 'some random status which doesnt exist'
) ORDER BY id;

-- case
-- what: .. WHERE <col> IN (<sq>) AND <col> IN (<sq on a table that isn't sharded>)
-- route select: 0,1
-- route select: 0,1
SELECT * FROM suborders WHERE id IN (
    SELECT id FROM subusers WHERE status = 'VIP'
) AND state IN (
    SELECT state FROM substates WHERE shipping = 'quick'
) ORDER BY id;

-- case
-- what: .. WHERE <col> IN (WITH c AS (..) SELECT .. FROM c)
-- route select: 0,1
-- route select: 0,1
SELECT * FROM suborders WHERE id IN (
    WITH c AS (SELECT id FROM subusers WHERE status = 'VIP') SELECT id FROM c
) ORDER BY id;

-- case
-- what: .. WHERE <col> IN (WITH c AS (..) SELECT .. FROM c), where c returns no rows
-- route select: 0,1
-- route select: 0,1
SELECT * FROM suborders WHERE id IN (
    WITH c AS (SELECT id FROM subusers WHERE status = 'some random status which doesnt exist') SELECT id FROM c
) ORDER BY id;

-- case
-- what: .. WHERE <col> IN (WITH c AS (..) SELECT .. FROM c), c returns timestamptz column
-- route select: 0,1
-- route select: 0,1
SELECT * FROM suborders WHERE id IN (
    WITH c AS (SELECT id, created_at FROM suborders) SELECT id FROM c WHERE created_at > '2026-10-13'
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
-- sql: SELECT avg(id), count(id) AS __pgdog_count_col0, sum(id) AS __pgdog_sum_col0 FROM subusers WHERE id < 4
-- route select: 0,1
-- sql: SELECT * FROM suborders WHERE id = (SELECT u.c1 FROM unnest($1::pg_catalog._numeric) u(c1)) ORDER BY id
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
-- sql: SELECT id FROM subusers WHERE status = 'VIP'
-- route select: any
-- sql: SELECT count(*) FROM (SELECT u.c1 FROM unnest($1::pg_catalog._int8) u(c1)) s(id)
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
SELECT * FROM (
    SELECT * FROM subusers WHERE status IS NOT NULL
) s ORDER BY s.id DESC LIMIT 2;

-- case
-- what: SELECT * FROM (<sq>) s, <sq> returns an array column
-- route select: 0,1
-- route select: any
-- sql: SELECT * FROM (SELECT u.c1, ($2::pg_catalog._varchar)[u.c2 + 1:u.c2 + u.c3] FROM unnest($1::pg_catalog._int8, $3::pg_catalog._int4, $4::pg_catalog._int4) u(c1, c2, c3)) s(id, tags) ORDER BY id
SELECT * FROM (SELECT id, tags FROM subusers) s ORDER BY id;

-- case
-- what: SELECT COUNT(*) FROM (<sq>) s, <sq> only returns constants
-- used by: Rails, which counts at most N rows for .limit(N).count (e.g., FollowRequest.where(..).limit(40).count) with
--          SELECT COUNT(*) FROM (SELECT 1 AS one FROM .. LIMIT N) subquery_for_count
--          - https://github.com/rails/rails/blob/v7.1.4/activerecord/lib/active_record/relation/calculations.rb#L453-L458
--          - https://github.com/rails/rails/blob/v7.1.4/activerecord/lib/active_record/relation/calculations.rb#L626-L639
-- used by: Mastodon, which counts the pending follow requests sent to accounts when the accounts log in with FollowRequest.where(target_account: ..).limit(40).count
--          - https://github.com/mastodon/mastodon/blob/v4.3.0/app/serializers/rest/credential_account_serializer.rb#L17
-- route select: 0,1
-- route select: any
-- sql: SELECT count(*) FROM (SELECT 1 FROM unnest($1::pg_catalog._int4) u(c1)) subquery_for_count(one)
SELECT COUNT(*) FROM (SELECT 1 AS one FROM subusers WHERE status = 'VIP' LIMIT 40) subquery_for_count;

-- case
-- what: SELECT COUNT(*) FROM (<sq>) s, <sq> only returns constants & returns zero rows
-- used by: Mastodon, which counts the pending follow requests sent to accounts when the accounts log in
--          For accounts without pending follow requests (most accounts), the derived table (SELECT 1 AS one FROM follow_requests WHERE target_account_id = .. LIMIT 40) doesn't return any rows
--          - https://github.com/mastodon/mastodon/blob/v4.3.0/app/serializers/rest/credential_account_serializer.rb#L17
-- route select: 0,1
-- route select: any
-- sql: SELECT count(*) FROM (SELECT 1 FROM unnest($1::pg_catalog._int4) u(c1)) subquery_for_count(one) WHERE false
SELECT COUNT(*) FROM (SELECT 1 AS one FROM subusers WHERE status = 'nobody' LIMIT 40) subquery_for_count;

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

-- case
-- what: WITH c AS (..) SELECT .. WHERE <col> IN (SELECT .. FROM c)
-- route select: 0,1
-- route select: 0,1
WITH c AS (
    SELECT id FROM subusers WHERE status = 'VIP'
) SELECT * FROM suborders WHERE id IN (
    SELECT id FROM c
) ORDER BY id;

-- case
-- what: WITH c (a, b) AS (..) SELECT .. FROM c
-- route select: 0,1
-- route select: any
WITH c (a, b) AS (
    SELECT id, status FROM subusers
) SELECT a, b FROM c ORDER BY a;

-- case
-- what: WITH c AS (SELECT .. FROM <st1>) SELECT <col>, <agg>(..) FROM c GROUP BY <col>
-- route select: 0,1
-- route select: any
WITH c AS (
    SELECT id, status FROM subusers
) SELECT status, count(*) FROM c GROUP BY status ORDER BY status;

-- case
-- what: WITH a AS (..), b AS (SELECT .. FROM a) .. FROM a JOIN b .., one CTE over another
-- route select: 0,1
-- route select: any
WITH a AS (
    SELECT id, status FROM subusers
), b AS (
    SELECT id FROM a WHERE status = 'VIP'
) SELECT a.id, a.status FROM a JOIN b ON a.id = b.id ORDER BY a.id;

-- case
-- what: WITH a AS (..), b AS (SELECT .. FROM a) .. FROM a JOIN b .., same CTE referenced more than once
-- route select: 0,1
-- route select: any
WITH a AS (
    SELECT id, status FROM subusers
) SELECT x.id, y.id FROM a x JOIN a y ON x.status = y.status ORDER BY x.id, y.id;
