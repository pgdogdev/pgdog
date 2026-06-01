use std::time::Duration;

use cache::*;
use serial_test::serial;
use sqlx::Executor;
use tokio::time::sleep;

/// Verifies that a second identical SELECT is served from Redis instead of PostgreSQL.
///
/// Strategy:
/// 1. Create and populate a test table.
/// 2. `no_cache` SELECT to verify the row exists in PG (does not warm Redis).
/// 3. Normal SELECT through pgdog (cache miss → response stored in Redis).
/// 4. Delete the row *directly* on PostgreSQL port 5432 so Redis is not invalidated.
/// 5. Normal SELECT again — must return the cached row even though PG has none.
#[tokio::test]
#[serial]
async fn test_cache_hit() {
    require_redis!();
    let pool = connection().await;

    pool.execute("CREATE TABLE IF NOT EXISTS cache_test_hit (id BIGINT PRIMARY KEY, val TEXT)")
        .await
        .unwrap();
    pool.execute("TRUNCATE cache_test_hit").await.unwrap();
    pool.execute("INSERT INTO cache_test_hit VALUES (1, 'hello')")
        .await
        .unwrap();

    // Confirm the row is there without touching the Redis cache.
    let rows: Vec<(i64, String)> = sqlx::query_as(
        "/* pgdog_cache: no_cache */ SELECT id, val FROM cache_test_hit WHERE id = 1",
    )
    .fetch_all(&pool)
    .await
    .unwrap();
    assert_eq!(rows.len(), 1, "row must exist in PG before cache warm-up");

    // Warm up the Redis cache with a regular (cacheable) SELECT.
    let first: Vec<(i64, String)> =
        sqlx::query_as("SELECT id, val FROM cache_test_hit WHERE id = 1")
            .fetch_all(&pool)
            .await
            .unwrap();
    assert_eq!(first.len(), 1, "first SELECT must return the row");

    // Delete the row directly in Postgres, bypassing pgdog so Redis is not invalidated.
    let direct = connection_direct().await;
    direct
        .execute("DELETE FROM cache_test_hit WHERE id = 1")
        .await
        .unwrap();
    direct.close().await;

    // Confirm the row is actually gone in PG (no_cache hint bypasses Redis).
    let gone: Vec<(i64, String)> = sqlx::query_as(
        "/* pgdog_cache: no_cache */ SELECT id, val FROM cache_test_hit WHERE id = 1",
    )
    .fetch_all(&pool)
    .await
    .unwrap();
    assert!(
        gone.is_empty(),
        "row must be gone in PG after direct delete"
    );

    // Now the same query through pgdog without a hint: must be served from Redis.
    let cached: Vec<(i64, String)> =
        sqlx::query_as("SELECT id, val FROM cache_test_hit WHERE id = 1")
            .fetch_all(&pool)
            .await
            .unwrap();
    assert_eq!(
        cached.len(),
        1,
        "cached SELECT must still return the row served from Redis"
    );
    assert_eq!(cached[0].1, "hello");

    pool.execute("DROP TABLE IF EXISTS cache_test_hit")
        .await
        .unwrap();
    pool.close().await;
}

/// Verifies that queries inside an explicit transaction are never served from Redis.
///
/// The cache must be bypassed for in-transaction queries so that the client
/// always sees the latest database state as part of its own transaction.
#[tokio::test]
#[serial]
async fn test_cache_bypassed_in_transaction() {
    require_redis!();
    let pool = connection().await;

    pool.execute("CREATE TABLE IF NOT EXISTS cache_test_txn (id BIGINT PRIMARY KEY, val TEXT)")
        .await
        .unwrap();
    pool.execute("TRUNCATE cache_test_txn").await.unwrap();
    pool.execute("INSERT INTO cache_test_txn VALUES (1, 'original')")
        .await
        .unwrap();

    // Warm up the cache.
    let _: Vec<(i64, String)> = sqlx::query_as("SELECT id, val FROM cache_test_txn WHERE id = 1")
        .fetch_all(&pool)
        .await
        .unwrap();

    // Inside a transaction, update the row and then SELECT — must see the updated value,
    // not the stale cached one.
    let mut tx = pool.begin().await.unwrap();
    sqlx::query("UPDATE cache_test_txn SET val = 'updated' WHERE id = 1")
        .execute(&mut *tx)
        .await
        .unwrap();

    let in_tx: Vec<(i64, String)> =
        sqlx::query_as("SELECT id, val FROM cache_test_txn WHERE id = 1")
            .fetch_all(&mut *tx)
            .await
            .unwrap();
    assert_eq!(
        in_tx[0].1, "updated",
        "SELECT inside a transaction must see the transaction's own write, not the Redis cache"
    );

    tx.commit().await.unwrap();

    pool.execute("DROP TABLE IF EXISTS cache_test_txn")
        .await
        .unwrap();
    pool.close().await;
}

/// Verifies that cached results expire after the configured TTL.
#[tokio::test]
#[serial]
async fn test_cache_ttl_expiry() {
    require_redis!();
    let pool = connection().await;

    pool.execute("CREATE TABLE IF NOT EXISTS cache_test_ttl (id BIGINT PRIMARY KEY, val TEXT)")
        .await
        .unwrap();
    pool.execute("TRUNCATE cache_test_ttl").await.unwrap();
    pool.execute("INSERT INTO cache_test_ttl VALUES (1, 'original')")
        .await
        .unwrap();

    // Warm up Redis.
    let _: Vec<(i64, String)> = sqlx::query_as(
        "/* pgdog_cache: cache ttl=1 */ SELECT id, val FROM cache_test_ttl WHERE id = 1",
    )
    .fetch_all(&pool)
    .await
    .unwrap();

    // Remove the row directly from PG so Redis is stale.
    let direct = connection_direct().await;
    direct
        .execute("DELETE FROM cache_test_ttl WHERE id = 1")
        .await
        .unwrap();
    direct.close().await;

    // Wait for the Redis entry to expire
    sleep(Duration::from_secs(2)).await;

    // After expiry pgdog must query PG and return no rows.
    let rows: Vec<(i64, String)> = sqlx::query_as(
        "/* pgdog_cache: cache ttl=1 */ SELECT id, val FROM cache_test_ttl WHERE id = 1",
    )
    .fetch_all(&pool)
    .await
    .unwrap();
    assert!(
        rows.is_empty(),
        "after TTL expiry the cached row must no longer be returned"
    );

    pool.execute("DROP TABLE IF EXISTS cache_test_ttl")
        .await
        .unwrap();
    pool.close().await;
}

/// Verifies that the extended protocol (parameterized `$1` queries) uses the
/// bind parameter values in the cache key.
#[tokio::test]
#[serial]
async fn test_extended_protocol_different_params_have_different_cache_keys() {
    require_redis!();
    let pool = connection().await;

    pool.execute("CREATE TABLE IF NOT EXISTS cache_test_ext (id BIGINT PRIMARY KEY, val TEXT)")
        .await
        .unwrap();
    pool.execute("TRUNCATE cache_test_ext").await.unwrap();
    pool.execute("INSERT INTO cache_test_ext VALUES (1, 'one'), (2, 'two')")
        .await
        .unwrap();

    // Warm cache for id=1.
    let r1: Vec<(i64, String)> = sqlx::query_as("SELECT id, val FROM cache_test_ext WHERE id = $1")
        .bind(1i64)
        .fetch_all(&pool)
        .await
        .unwrap();
    assert_eq!(r1.len(), 1);
    assert_eq!(r1[0].1, "one");

    // Warm cache for id=2.
    let r2: Vec<(i64, String)> = sqlx::query_as("SELECT id, val FROM cache_test_ext WHERE id = $1")
        .bind(2i64)
        .fetch_all(&pool)
        .await
        .unwrap();
    assert_eq!(r2.len(), 1);
    assert_eq!(r2[0].1, "two");

    // Delete both rows directly in PG so that any result must come from Redis.
    let direct = connection_direct().await;
    direct.execute("DELETE FROM cache_test_ext").await.unwrap();
    direct.close().await;

    let cached1: Vec<(i64, String)> =
        sqlx::query_as("SELECT id, val FROM cache_test_ext WHERE id = $1")
            .bind(1i64)
            .fetch_all(&pool)
            .await
            .unwrap();
    assert_eq!(cached1.len(), 1, "id=1 entry must be served from cache");
    assert_eq!(cached1[0].1, "one");

    let cached2: Vec<(i64, String)> =
        sqlx::query_as("SELECT id, val FROM cache_test_ext WHERE id = $1")
            .bind(2i64)
            .fetch_all(&pool)
            .await
            .unwrap();
    assert_eq!(cached2.len(), 1, "id=2 entry must be served from cache");
    assert_eq!(cached2[0].1, "two");

    pool.execute("DROP TABLE IF EXISTS cache_test_ext")
        .await
        .unwrap();
    pool.close().await;
}

/// Verifies that `/* pgdog_cache: force_cache */` updates cache.
#[tokio::test]
#[serial]
async fn test_force_cache() {
    require_redis!();
    let pool = connection().await;

    pool.execute("CREATE TABLE IF NOT EXISTS cache_test_force (id BIGINT PRIMARY KEY, val TEXT)")
        .await
        .unwrap();
    pool.execute("TRUNCATE cache_test_force").await.unwrap();
    pool.execute("INSERT INTO cache_test_force VALUES (1, 'not_forced')")
        .await
        .unwrap();

    // Warm cache
    let r1: Vec<(i64, String)> = sqlx::query_as(
        "/* pgdog_cache: cache ttl=2 */ SELECT id, val FROM cache_test_force WHERE id = 1",
    )
    .fetch_all(&pool)
    .await
    .unwrap();
    assert_eq!(r1.len(), 1);
    assert_eq!(r1[0].1, "not_forced");

    let direct = connection_direct().await;
    direct
        .execute("UPDATE cache_test_force SET val = 'forced' WHERE id = 1")
        .await
        .unwrap();
    direct.close().await;

    let r: Vec<(i64, String)> = sqlx::query_as(
        "/* pgdog_cache: force_cache ttl=3 */ SELECT id, val FROM cache_test_force WHERE id = 1",
    )
    .fetch_all(&pool)
    .await
    .unwrap();
    assert_eq!(r.len(), 1);
    assert_eq!(r[0].1, "forced");

    let cached: Vec<(i64, String)> =
        sqlx::query_as("SELECT id, val FROM cache_test_force WHERE id = 1")
            .fetch_all(&pool)
            .await
            .unwrap();
    assert_eq!(cached.len(), 1);
    assert_eq!(cached[0].1, "forced");

    pool.execute("DROP TABLE IF EXISTS cache_test_force")
        .await
        .unwrap();
    pool.close().await;
}

/// Verifies that `/* pgdog_cache: no_cache */` prevents the response from being
/// stored in Redis, so a subsequent plain SELECT actually hits PostgreSQL.
#[tokio::test]
#[serial]
async fn test_no_cache_hint_does_not_warm_redis() {
    require_redis!();
    let pool = connection().await;

    pool.execute("CREATE TABLE IF NOT EXISTS cache_test_no_warm (id BIGINT PRIMARY KEY, val TEXT)")
        .await
        .unwrap();
    pool.execute("TRUNCATE cache_test_no_warm").await.unwrap();
    pool.execute("INSERT INTO cache_test_no_warm VALUES (1, 'original')")
        .await
        .unwrap();

    // Fetch with no_cache — must NOT warm Redis.
    let r: Vec<(i64, String)> = sqlx::query_as(
        "/* pgdog_cache: no_cache */ SELECT id, val FROM cache_test_no_warm WHERE id = 1",
    )
    .fetch_all(&pool)
    .await
    .unwrap();
    assert_eq!(r.len(), 1);

    let direct = connection_direct().await;
    direct
        .execute("DELETE FROM cache_test_no_warm WHERE id = 1")
        .await
        .unwrap();
    direct.close().await;

    // A plain SELECT must reach PG (no cache entry) and return 0 rows.
    let after: Vec<(i64, String)> =
        sqlx::query_as("SELECT id, val FROM cache_test_no_warm WHERE id = 1")
            .fetch_all(&pool)
            .await
            .unwrap();
    assert!(
        after.is_empty(),
        "no_cache hint must not warm Redis, so PG miss returns 0 rows"
    );

    pool.execute("DROP TABLE IF EXISTS cache_test_no_warm")
        .await
        .unwrap();
    pool.close().await;
}

/// Verifies that passing `pgdog.cache=no_cache` in the connection DSN options
/// bypasses the cache for all queries on that connection.
#[tokio::test]
#[serial]
async fn test_connection_option_no_cache_bypasses_redis() {
    require_redis!();
    let pool = connection().await;

    pool.execute("CREATE TABLE IF NOT EXISTS cache_test_param (id BIGINT PRIMARY KEY, val TEXT)")
        .await
        .unwrap();
    pool.execute("TRUNCATE cache_test_param").await.unwrap();
    pool.execute("INSERT INTO cache_test_param VALUES (1, 'cached_val')")
        .await
        .unwrap();

    // Warm the cache via a normal connection.
    let _: Vec<(i64, String)> = sqlx::query_as("SELECT id, val FROM cache_test_param WHERE id = 1")
        .fetch_all(&pool)
        .await
        .unwrap();

    let direct = connection_direct().await;
    direct
        .execute("DELETE FROM cache_test_param WHERE id = 1")
        .await
        .unwrap();
    direct.close().await;

    // A connection with pgdog.cache=no_cache must bypass Redis and hit PG,
    // returning 0 rows because the row was deleted.
    let no_cache_conn = connection_with_options("-c%20pgdog.cache%3Dno_cache").await;
    let rows: Vec<(i64, String)> =
        sqlx::query_as("SELECT id, val FROM cache_test_param WHERE id = 1")
            .fetch_all(&no_cache_conn)
            .await
            .unwrap();
    assert!(
        rows.is_empty(),
        "connection-level no_cache must bypass Redis and see the deleted row"
    );
    no_cache_conn.close().await;

    pool.execute("DROP TABLE IF EXISTS cache_test_param")
        .await
        .unwrap();
    pool.close().await;
}

/// Verifies that error responses are never stored in Redis.
///
/// A query that initially errors must not poison the cache: after the error
/// is fixed (table created), the same query must reach PG and return live data.
#[tokio::test]
#[serial]
async fn test_error_response_not_cached() {
    require_redis!();
    let pool = connection().await;

    pool.execute("DROP TABLE IF EXISTS cache_test_error")
        .await
        .unwrap();

    // This SELECT will produce an error (table does not exist).
    let err = sqlx::query("SELECT id, val FROM cache_test_error WHERE id = 1")
        .fetch_all(&pool)
        .await;
    assert!(err.is_err(), "query on missing table must return an error");

    // Now create the table and insert a row.
    pool.execute("CREATE TABLE cache_test_error (id BIGINT PRIMARY KEY, val TEXT)")
        .await
        .unwrap();
    pool.execute("INSERT INTO cache_test_error VALUES (1, 'live')")
        .await
        .unwrap();

    // The same query must now hit PG (the previous error was not cached).
    let rows: Vec<(i64, String)> =
        sqlx::query_as("SELECT id, val FROM cache_test_error WHERE id = 1")
            .fetch_all(&pool)
            .await
            .unwrap();
    assert_eq!(
        rows.len(),
        1,
        "error must not be cached; must return live row"
    );
    assert_eq!(rows[0].1, "live");

    pool.execute("DROP TABLE IF EXISTS cache_test_error")
        .await
        .unwrap();
    pool.close().await;
}
