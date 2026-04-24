\set id random(1, 10000000)

/* pgdog_shard: 0 */ SELECT :id;

SELECT :id /* pgdog_role: replica */;

/* trace_id=abc-123 request_id=xyz-456 */ SELECT :id;

SELECT :id;
