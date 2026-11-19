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
-- sql: SELECT avg(id), count(id) AS __pgdog_count_col0 FROM subusers WHERE id < 4
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
-- what: .. WHERE <col> IN (<sq>), <sq> returns an error
-- route select: 0,1
-- sql: SELECT 1 / 0 FROM subusers
SELECT * FROM subusers WHERE id IN (SELECT 1 / 0 FROM subusers);

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

-- case
-- what: .. FROM subusers u JOIN suborders o ON u.id = o.user_id (the sharding keys of both tables)
-- used by: Mastodon, which joins accounts to users with Account.joins(:user) on accounts.id = users.account_id
--          - https://github.com/mastodon/mastodon/blob/v4.3.0/app/lib/feed_manager.rb#L21
-- route select: 0,1
-- sql: SELECT u.id, u.username, o.id AS order_id FROM subusers u JOIN suborders o ON u.id = o.user_id ORDER BY o.id
SELECT u.id, u.username, o.id AS order_id FROM subusers u
JOIN suborders o ON u.id = o.user_id
ORDER BY o.id;

-- case
-- what: JOIN on suborders.id = subusers.id (the sharding key of suborders is user_id, not id)
-- used by: Mastodon, which joins notifications to the accounts that sent the notifications on accounts.id = notifications.from_account_id
--          - https://github.com/mastodon/mastodon/blob/v4.3.0/app/models/notification.rb#L108
-- route select: 0,1
-- sql: SELECT * FROM subusers u
-- route select: 0,1
-- sql: SELECT o.id, o.user_id, u.username FROM suborders o JOIN (SELECT u.c1, u.c2, u.c3, u.c4, u.c5, u.c6, ($7::pg_catalog._varchar)[u.c7 + 1:u.c7 + u.c8], ($10::pg_catalog._int8)[u.c9 + 1:u.c9 + u.c10] FROM unnest($1::pg_catalog._int8, $2::pg_catalog._text, $3::pg_catalog._text, $4::pg_catalog._jsonb, $5::pg_catalog._date, $6::pg_catalog._inet, $8::pg_catalog._int4, $9::pg_catalog._int4, $11::pg_catalog._int4, $12::pg_catalog._int4) u(c1, c2, c3, c4, c5, c6, c7, c8, c9, c10)) u(id, username, status, profile, signed_up, last_ip, tags, media_ids) ON o.id = u.id ORDER BY o.id
SELECT o.id, o.user_id, u.username FROM suborders o
JOIN subusers u ON o.id = u.id
ORDER BY o.id;

-- case
-- what: JOIN on x.status = y.status (the sharding key of subusers is id)
-- used by: Mastodon, which joins follows to the followed accounts on follows.target_account_id = target_accounts.id to suggest accounts to follow
--          - https://github.com/mastodon/mastodon/blob/v4.3.0/app/models/account_suggestions/friends_of_friends_source.rb#L14
-- route select: 0,1
-- sql: SELECT * FROM subusers y
-- route select: 0,1
-- sql: SELECT x.id, y.id, x.id AS __pgdog_order_col0, y.id AS __pgdog_order_col1 FROM subusers x JOIN (SELECT u.c1, u.c2, u.c3, u.c4, u.c5, u.c6, ($7::pg_catalog._varchar)[u.c7 + 1:u.c7 + u.c8], ($10::pg_catalog._int8)[u.c9 + 1:u.c9 + u.c10] FROM unnest($1::pg_catalog._int8, $2::pg_catalog._text, $3::pg_catalog._text, $4::pg_catalog._jsonb, $5::pg_catalog._date, $6::pg_catalog._inet, $8::pg_catalog._int4, $9::pg_catalog._int4, $11::pg_catalog._int4, $12::pg_catalog._int4) u(c1, c2, c3, c4, c5, c6, c7, c8, c9, c10)) y(id, username, status, profile, signed_up, last_ip, tags, media_ids) ON x.status = y.status ORDER BY x.id, y.id
SELECT x.id, y.id FROM subusers x
JOIN subusers y ON x.status = y.status
ORDER BY x.id, y.id;

-- case
-- what: LEFT JOIN on x.status = y.status (the sharding key of subusers is id)
-- used by: Mastodon, which LEFT JOINs accounts to follows on follows.target_account_id = accounts.id to show admins the most followed accounts of a server
--          - https://github.com/mastodon/mastodon/blob/v4.3.0/app/lib/admin/metrics/dimension/instance_accounts_dimension.rb#L27-L33
-- route select: 0,1
-- sql: SELECT * FROM subusers y
-- route select: 0,1
-- sql: SELECT x.id, y.id, x.id AS __pgdog_order_col0, y.id AS __pgdog_order_col1 FROM subusers x LEFT JOIN (SELECT u.c1, u.c2, u.c3, u.c4, u.c5, u.c6, ($7::pg_catalog._varchar)[u.c7 + 1:u.c7 + u.c8], ($10::pg_catalog._int8)[u.c9 + 1:u.c9 + u.c10] FROM unnest($1::pg_catalog._int8, $2::pg_catalog._text, $3::pg_catalog._text, $4::pg_catalog._jsonb, $5::pg_catalog._date, $6::pg_catalog._inet, $8::pg_catalog._int4, $9::pg_catalog._int4, $11::pg_catalog._int4, $12::pg_catalog._int4) u(c1, c2, c3, c4, c5, c6, c7, c8, c9, c10)) y(id, username, status, profile, signed_up, last_ip, tags, media_ids) ON x.status = y.status ORDER BY x.id, y.id
SELECT x.id, y.id FROM subusers x
LEFT JOIN subusers y ON x.status = y.status
ORDER BY x.id, y.id;

-- case
-- what: .. FROM subusers x CROSS JOIN subusers y
-- used by: not Mastodon or Rails, but CROSS JOIN was free to add, since Postgres parses CROSS JOIN as an inner JOIN without an ON clause
-- route select: 0,1
-- sql: SELECT * FROM subusers y
-- route select: 0,1
-- sql: SELECT x.id, y.id FROM subusers x CROSS JOIN (SELECT u.c1, u.c2, u.c3, u.c4, u.c5, u.c6, ($7::pg_catalog._varchar)[u.c7 + 1:u.c7 + u.c8], ($10::pg_catalog._int8)[u.c9 + 1:u.c9 + u.c10] FROM unnest($1::pg_catalog._int8, $2::pg_catalog._text, $3::pg_catalog._text, $4::pg_catalog._jsonb, $5::pg_catalog._date, $6::pg_catalog._inet, $8::pg_catalog._int4, $9::pg_catalog._int4, $11::pg_catalog._int4, $12::pg_catalog._int4) u(c1, c2, c3, c4, c5, c6, c7, c8, c9, c10)) y(id, username, status, profile, signed_up, last_ip, tags, media_ids) ORDER BY 1, 2
SELECT x.id, y.id FROM subusers x
CROSS JOIN subusers y
ORDER BY 1, 2;

-- case
-- what: .. FROM suborders o NATURAL JOIN subusers u (id is the only shared column between the two tables)
-- used by: not Mastodon or Rails, but NATURAL JOIN was free to add, since Postgres parses NATURAL JOIN as an inner JOIN on the shared columns between the two tables
-- route select: 0,1
-- sql: SELECT * FROM subusers u
-- route select: 0,1
-- sql: SELECT o.id, u.username FROM suborders o NATURAL JOIN (SELECT u.c1, u.c2, u.c3, u.c4, u.c5, u.c6, ($7::pg_catalog._varchar)[u.c7 + 1:u.c7 + u.c8], ($10::pg_catalog._int8)[u.c9 + 1:u.c9 + u.c10] FROM unnest($1::pg_catalog._int8, $2::pg_catalog._text, $3::pg_catalog._text, $4::pg_catalog._jsonb, $5::pg_catalog._date, $6::pg_catalog._inet, $8::pg_catalog._int4, $9::pg_catalog._int4, $11::pg_catalog._int4, $12::pg_catalog._int4) u(c1, c2, c3, c4, c5, c6, c7, c8, c9, c10)) u(id, username, status, profile, signed_up, last_ip, tags, media_ids) ORDER BY 1
SELECT o.id, u.username FROM suborders o
NATURAL JOIN subusers u
ORDER BY 1;

-- case
-- what: .. FROM suborders o JOIN substates s ON s.state = o.state (substates isn't sharded)
-- used by: Mastodon, which joins statuses to oauth_applications to find the apps that users post with the most
--          - https://github.com/mastodon/mastodon/blob/v4.3.0/app/lib/annual_report/most_used_apps.rb#L20
-- route select: 0,1
-- sql: SELECT o.id, s.shipping FROM suborders o JOIN substates s ON s.state = o.state ORDER BY o.id
SELECT o.id, s.shipping FROM suborders o
JOIN substates s ON s.state = o.state
ORDER BY o.id;

-- case
-- what: .. FROM "suborders" INNER JOIN "subusers" ON "subusers"."id" = "suborders"."id" (without an alias)
-- used by: Mastodon, which leaves out the notifications from suspended accounts with Notification.without_suspended, i.e., joins(:from_account), which Rails writes without an alias
--          - https://github.com/mastodon/mastodon/blob/v4.3.0/app/models/notification.rb#L108
-- route select: 0,1
-- sql: SELECT * FROM subusers
-- route select: 0,1
-- sql: SELECT suborders.id, subusers.username FROM suborders JOIN (SELECT u.c1, u.c2, u.c3, u.c4, u.c5, u.c6, ($7::pg_catalog._varchar)[u.c7 + 1:u.c7 + u.c8], ($10::pg_catalog._int8)[u.c9 + 1:u.c9 + u.c10] FROM unnest($1::pg_catalog._int8, $2::pg_catalog._text, $3::pg_catalog._text, $4::pg_catalog._jsonb, $5::pg_catalog._date, $6::pg_catalog._inet, $8::pg_catalog._int4, $9::pg_catalog._int4, $11::pg_catalog._int4, $12::pg_catalog._int4) u(c1, c2, c3, c4, c5, c6, c7, c8, c9, c10)) subusers(id, username, status, profile, signed_up, last_ip, tags, media_ids) ON subusers.id = suborders.id ORDER BY suborders.id
SELECT "suborders"."id", "subusers"."username" FROM "suborders"
INNER JOIN "subusers" ON "subusers"."id" = "suborders"."id"
ORDER BY "suborders"."id";

-- case
-- what: .. FROM suborders JOIN subusers ON .. WHERE subusers.id = 3
--       We use the name subusers as the alias of the fetched rows, so subusers.id = 3 refers to the fetched rows, not to the table subusers
--       The router doesn't route by subusers.id = 3, since the fetched rows are a derived table (#2064)
-- route select: 0,1
-- sql: SELECT * FROM subusers
-- route select: 0,1
-- sql: SELECT suborders.id, subusers.id FROM suborders JOIN (SELECT u.c1, u.c2, u.c3, u.c4, u.c5, u.c6, ($7::pg_catalog._varchar)[u.c7 + 1:u.c7 + u.c8], ($10::pg_catalog._int8)[u.c9 + 1:u.c9 + u.c10] FROM unnest($1::pg_catalog._int8, $2::pg_catalog._text, $3::pg_catalog._text, $4::pg_catalog._jsonb, $5::pg_catalog._date, $6::pg_catalog._inet, $8::pg_catalog._int4, $9::pg_catalog._int4, $11::pg_catalog._int4, $12::pg_catalog._int4) u(c1, c2, c3, c4, c5, c6, c7, c8, c9, c10)) subusers(id, username, status, profile, signed_up, last_ip, tags, media_ids) ON subusers.id = suborders.id WHERE subusers.id = 3
SELECT "suborders"."id", "subusers"."id" FROM "suborders"
INNER JOIN "subusers" ON "subusers"."id" = "suborders"."id"
WHERE "subusers"."id" = 3;

-- case
-- what: .. FROM "subusers" LEFT OUTER JOIN "suborders" ON "suborders"."id" = "subusers"."id" (without an alias)
-- used by: Mastodon, which LEFT JOINs statuses to mentions with account.statuses.left_outer_joins(:mentions) to list the posts of other accounts without boosts (exclude_reblogs=true)
--          - https://github.com/mastodon/mastodon/blob/v4.3.0/app/lib/account_statuses_filter.rb#L49
-- route select: 0,1
-- sql: SELECT * FROM suborders
-- route select: 0,1
-- sql: SELECT subusers.id, suborders.id FROM subusers LEFT JOIN (SELECT u.c1, u.c2, u.c3, u.c4 FROM unnest($1::pg_catalog._int8, $2::pg_catalog._int8, $3::pg_catalog._text, $4::pg_catalog._timestamptz) u(c1, c2, c3, c4)) suborders(id, user_id, state, created_at) ON suborders.id = subusers.id ORDER BY 1, 2
SELECT "subusers"."id", "suborders"."id" FROM "subusers"
LEFT OUTER JOIN "suborders" ON "suborders"."id" = "subusers"."id"
ORDER BY 1, 2;

-- case
-- what: .. FROM suborders JOIN subusers ON .. WHERE suborders.id IN (SELECT .. FROM subusers ..)
--       We rewrite this statement twice (first the subquery in WHERE, then the JOIN)
--       Without the `|=` in `send_query_string`, simple-protocol clients would receive ParseComplete and BindComplete before the rows
-- route select: 0,1
-- sql: SELECT subusers.id FROM subusers WHERE subusers.status = 'VIP'
-- route select: 0,1
-- sql: SELECT * FROM subusers
-- route select: 0,1
-- sql: SELECT suborders.id FROM suborders JOIN (SELECT u.c1, u.c2, u.c3, u.c4, u.c5, u.c6, ($8::pg_catalog._varchar)[u.c7 + 1:u.c7 + u.c8], ($11::pg_catalog._int8)[u.c9 + 1:u.c9 + u.c10] FROM unnest($2::pg_catalog._int8, $3::pg_catalog._text, $4::pg_catalog._text, $5::pg_catalog._jsonb, $6::pg_catalog._date, $7::pg_catalog._inet, $9::pg_catalog._int4, $10::pg_catalog._int4, $12::pg_catalog._int4, $13::pg_catalog._int4) u(c1, c2, c3, c4, c5, c6, c7, c8, c9, c10)) subusers(id, username, status, profile, signed_up, last_ip, tags, media_ids) ON subusers.id = suborders.id WHERE suborders.id IN (SELECT u.c1 FROM unnest($1::pg_catalog._int8) u(c1)) ORDER BY 1
SELECT "suborders"."id" FROM "suborders"
INNER JOIN "subusers" ON "subusers"."id" = "suborders"."id"
WHERE "suborders"."id" IN (SELECT "subusers"."id" FROM "subusers" WHERE "subusers"."status" = 'VIP')
ORDER BY 1;

-- case
-- what: .. FROM suborders o JOIN subusers u ON .. WHERE o.user_id = 2 (o.user_id is the sharding key of suborders)
-- used by: Mastodon, which lists the notifications of the current account with current_account.notifications.without_suspended (WHERE notifications.account_id = ..)
--          - https://github.com/mastodon/mastodon/blob/v4.3.0/app/controllers/api/v1/notifications_controller.rb#L59
-- route select: 0,1
-- sql: SELECT * FROM subusers u
-- route select: 0
-- sql: SELECT o.id, u.id FROM suborders o JOIN (SELECT u.c1, u.c2, u.c3, u.c4, u.c5, u.c6, ($7::pg_catalog._varchar)[u.c7 + 1:u.c7 + u.c8], ($10::pg_catalog._int8)[u.c9 + 1:u.c9 + u.c10] FROM unnest($1::pg_catalog._int8, $2::pg_catalog._text, $3::pg_catalog._text, $4::pg_catalog._jsonb, $5::pg_catalog._date, $6::pg_catalog._inet, $8::pg_catalog._int4, $9::pg_catalog._int4, $11::pg_catalog._int4, $12::pg_catalog._int4) u(c1, c2, c3, c4, c5, c6, c7, c8, c9, c10)) u(id, username, status, profile, signed_up, last_ip, tags, media_ids) ON u.id = o.id WHERE o.user_id = 2 ORDER BY 1, 2
SELECT o.id, u.id FROM suborders o
JOIN subusers u ON u.id = o.id
WHERE o.user_id = 2
ORDER BY 1, 2;

-- case
-- what: .. FROM subusers x LEFT JOIN subpackages y ON x.id = y.order_id (subpackages doesn't have any rows)
--       We send empty unnest(..) arrays in place of subpackages y
-- route select: 0,1
-- sql: SELECT * FROM subpackages y
-- route select: 0,1
-- sql: SELECT x.id, y.id FROM subusers x LEFT JOIN (SELECT u.c1, u.c2, u.c3, u.c4, u.c5 FROM unnest($1::pg_catalog._int8, $2::pg_catalog._int8, $3::pg_catalog._int8, $4::pg_catalog._timestamptz, $5::pg_catalog._timestamptz) u(c1, c2, c3, c4, c5)) y(id, user_id, order_id, shipped_at, estimated_arrival_time) ON x.id = y.order_id ORDER BY 1, 2
SELECT x.id, y.id FROM subusers x
LEFT JOIN subpackages y ON x.id = y.order_id
ORDER BY 1, 2;

-- case
-- what: JOIN on x.status = y.status with parameters in ON and WHERE
-- route select: 0,1
-- sql: SELECT * FROM subusers y
-- route select: 0
-- sql: SELECT x.id, y.id FROM subusers x JOIN (SELECT u.c1, u.c2, u.c3, u.c4, u.c5, u.c6, ($9::pg_catalog._varchar)[u.c7 + 1:u.c7 + u.c8], ($12::pg_catalog._int8)[u.c9 + 1:u.c9 + u.c10] FROM unnest($3::pg_catalog._int8, $4::pg_catalog._text, $5::pg_catalog._text, $6::pg_catalog._jsonb, $7::pg_catalog._date, $8::pg_catalog._inet, $10::pg_catalog._int4, $11::pg_catalog._int4, $13::pg_catalog._int4, $14::pg_catalog._int4) u(c1, c2, c3, c4, c5, c6, c7, c8, c9, c10)) y(id, username, status, profile, signed_up, last_ip, tags, media_ids) ON x.status = y.status AND y.username <> $2 WHERE x.id = $1 ORDER BY 1, 2
-- $1 = 1
-- $2 = nobody
SELECT x.id, y.id FROM subusers x
JOIN subusers y ON x.status = y.status AND y.username <> $2
WHERE x.id = $1
ORDER BY 1, 2;

-- case
-- what: .. WHERE id <= $1 AND id IN (SELECT .. FROM suborders o JOIN subusers u ON .. WHERE o.id >= $2)
-- route select: 0,1
-- sql: SELECT * FROM subusers u
-- route select: 0,1
-- sql: SELECT u.id FROM suborders o JOIN (SELECT u.c1, u.c2, u.c3, u.c4, u.c5, u.c6, ($8::pg_catalog._varchar)[u.c7 + 1:u.c7 + u.c8], ($11::pg_catalog._int8)[u.c9 + 1:u.c9 + u.c10] FROM unnest($2::pg_catalog._int8, $3::pg_catalog._text, $4::pg_catalog._text, $5::pg_catalog._jsonb, $6::pg_catalog._date, $7::pg_catalog._inet, $9::pg_catalog._int4, $10::pg_catalog._int4, $12::pg_catalog._int4, $13::pg_catalog._int4) u(c1, c2, c3, c4, c5, c6, c7, c8, c9, c10)) u(id, username, status, profile, signed_up, last_ip, tags, media_ids) ON o.id = u.id WHERE o.id >= $1
-- route select: 0,1
-- sql: SELECT id FROM subusers WHERE id <= $1 AND id IN (SELECT u.c1 FROM unnest($2::pg_catalog._int8) u(c1)) ORDER BY id
-- $1 = 3
-- $2 = 2
SELECT id FROM subusers
WHERE id <= $1 AND id IN (SELECT u.id FROM suborders o JOIN subusers u ON o.id = u.id WHERE o.id >= $2)
ORDER BY id;

