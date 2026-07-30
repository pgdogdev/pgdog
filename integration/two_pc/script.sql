\set id random(1, 9223372036854775807)
\set value random(1, 9223372036854775807)

BEGIN;
INSERT INTO omnisharded_two_pc (id, value)
VALUES (:id, format('pgbench-client-%s-value-%s', :client_id, :value));
COMMIT;
