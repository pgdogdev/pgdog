/* pgdog_shard: 0 */
SELECT
    pgdog.install_next_id ('public', 'kv', 'id', 3, 0);

/* pgdog_shard: 1 */
SELECT
    pgdog.install_next_id ('public', 'kv', 'id', 3, 1);

/* pgdog_shard: 2 */
SELECT
    pgdog.install_next_id ('public', 'kv', 'id', 3, 2);
