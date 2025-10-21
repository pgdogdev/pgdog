use rust::setup::{admin_sqlx, connections_sqlx};
use sqlx::{Executor, Pool, Postgres};

const TEST_TABLE: &str = "sharded_list";
const NON_SHARDED_TABLE: &str = "shard_key_rewrite_non_sharded";

struct RewriteConfigGuard {
    admin: Pool<Postgres>,
}

impl RewriteConfigGuard {
    async fn enable(admin: Pool<Postgres>) -> Self {
        admin
            .execute("SET two_phase_commit TO false")
            .await
            .expect("disable two_phase_commit");
        admin
            .execute("SET rewrite_shard_key_updates TO rewrite")
            .await
            .expect("enable shard key rewrite");
        Self { admin }
    }
}

impl Drop for RewriteConfigGuard {
    fn drop(&mut self) {
        let admin = self.admin.clone();
        tokio::spawn(async move {
            let _ = admin
                .execute("SET rewrite_shard_key_updates TO ignore")
                .await;
            let _ = admin.execute("SET two_phase_commit TO false").await;
        });
    }
}

#[tokio::test]
async fn shard_key_insert_rewrite_is_pending_for_multi_row_inserts() {
    let admin = admin_sqlx().await;
    let _guard = RewriteConfigGuard::enable(admin.clone()).await;

    let mut pools = connections_sqlx().await;
    let sharded = pools.swap_remove(1);

    cleanup_table(&sharded).await;

    let err = sqlx::query(
        "INSERT INTO sharded (id, value) VALUES (1, 'one'), (2, 'two')",
    )
    .execute(&sharded)
    .await
    .expect_err("multi-row insert into sharded table should fail for now");

    let db_err = err
        .as_database_error()
        .expect("expected database error from proxy");
    let message = db_err.message();
    assert!(
        message.contains("multiple-row INSERT into sharded table is not supported")
            || message.contains("multi-row insert not supported"),
        "unexpected error message: {}",
        message
    );

    verify_sharded_table_empty(&sharded).await;
}

#[tokio::test]
async fn shard_key_insert_rewrite_allows_multi_row_inserts_on_non_sharded_tables() {
    let admin = admin_sqlx().await;
    let _guard = RewriteConfigGuard::enable(admin.clone()).await;

    let mut pools = connections_sqlx().await;
    let sharded = pools.swap_remove(1);

    prepare_non_sharded_table(&sharded).await;

    sqlx::query(format!(
        "INSERT INTO {NON_SHARDED_TABLE} (id, name) VALUES (1, 'one'), (2, 'two')"
    ).as_str())
    .execute(&sharded)
    .await
    .expect("multi-row insert should succeed for non-sharded table");

    let rows: Vec<(i64, String)> = sqlx::query_as(&format!(
        "SELECT id, name FROM {NON_SHARDED_TABLE} ORDER BY id"
    ))
    .fetch_all(&sharded)
    .await
    .expect("read back non-sharded rows");
    assert_eq!(rows.len(), 2, "expected two rows in non-sharded table");
    assert_eq!(rows[0], (1, "one".to_string()));
    assert_eq!(rows[1], (2, "two".to_string()));

    cleanup_non_sharded_table(&sharded).await;
}

#[tokio::test]
async fn shard_key_update_rewrite_moves_row_between_shards() {
    let admin = admin_sqlx().await;
    let _guard = RewriteConfigGuard::enable(admin.clone()).await;

    let mut pools = connections_sqlx().await;
    let pool = pools.swap_remove(1);

    prepare_table(&pool).await;

    let insert = format!("INSERT INTO {TEST_TABLE} (id, value) VALUES (1, 'old')");
    pool.execute(insert.as_str())
        .await
        .expect("insert initial row");

    assert_eq!(count_on_shard(&pool, 0, 1).await, 1, "row on shard 0");
    assert_eq!(count_on_shard(&pool, 1, 1).await, 0, "no row on shard 1");

    let update = format!("UPDATE {TEST_TABLE} SET id = 11 WHERE id = 1");
    let result = pool.execute(update.as_str()).await.expect("rewrite update");
    assert_eq!(result.rows_affected(), 1, "exactly one row updated");

    assert_eq!(
        count_on_shard(&pool, 0, 1).await,
        0,
        "row removed from shard 0"
    );
    assert_eq!(
        count_on_shard(&pool, 1, 11).await,
        1,
        "row inserted on shard 1"
    );

    cleanup_table(&pool).await;
}

#[tokio::test]
async fn shard_key_update_rewrite_rejects_multiple_rows() {
    let admin = admin_sqlx().await;
    let _guard = RewriteConfigGuard::enable(admin.clone()).await;

    let mut pools = connections_sqlx().await;
    let pool = pools.swap_remove(1);

    prepare_table(&pool).await;

    let insert_first = format!("INSERT INTO {TEST_TABLE} (id, value) VALUES (1, 'old')");
    pool.execute(insert_first.as_str())
        .await
        .expect("insert first row");

    let insert_second = format!("INSERT INTO {TEST_TABLE} (id, value) VALUES (2, 'older')");
    pool.execute(insert_second.as_str())
        .await
        .expect("insert second row");

    let update = format!("UPDATE {TEST_TABLE} SET id = 11 WHERE id IN (1, 2)");
    let err = pool
        .execute(update.as_str())
        .await
        .expect_err("expected multi-row rewrite to fail");
    let db_err = err
        .as_database_error()
        .expect("expected database error from proxy");
    assert!(
        db_err
            .message()
            .contains("updating multiple rows is not supported when updating the sharding key"),
        "unexpected error message: {}",
        db_err.message()
    );

    assert_eq!(
        count_on_shard(&pool, 0, 1).await,
        1,
        "row 1 still on shard 0"
    );
    assert_eq!(
        count_on_shard(&pool, 0, 2).await,
        1,
        "row 2 still on shard 0"
    );
    assert_eq!(
        count_on_shard(&pool, 1, 11).await,
        0,
        "no row inserted on shard 1"
    );

    cleanup_table(&pool).await;
}

#[tokio::test]
async fn shard_key_update_rewrite_rejects_transactions() {
    let admin = admin_sqlx().await;
    let _guard = RewriteConfigGuard::enable(admin.clone()).await;

    let mut pools = connections_sqlx().await;
    let pool = pools.swap_remove(1);

    prepare_table(&pool).await;

    let insert = format!("INSERT INTO {TEST_TABLE} (id, value) VALUES (1, 'old')");
    pool.execute(insert.as_str())
        .await
        .expect("insert initial row");

    let mut conn = pool.acquire().await.expect("acquire connection");
    conn.execute("BEGIN").await.expect("begin transaction");

    let update = format!("UPDATE {TEST_TABLE} SET id = 11 WHERE id = 1");
    let err = conn
        .execute(update.as_str())
        .await
        .expect_err("rewrite inside transaction must fail");
    let db_err = err
        .as_database_error()
        .expect("expected database error from proxy");
    assert!(
        db_err
            .message()
            .contains("shard key rewrites must run outside explicit transactions"),
        "unexpected error message: {}",
        db_err.message()
    );

    conn.execute("ROLLBACK").await.ok();

    drop(conn);

    assert_eq!(count_on_shard(&pool, 0, 1).await, 1, "row still on shard 0");
    assert_eq!(
        count_on_shard(&pool, 1, 11).await,
        0,
        "no row inserted on shard 1"
    );

    cleanup_table(&pool).await;
}

async fn prepare_table(pool: &Pool<Postgres>) {
    for shard in [0, 1] {
        let drop = format!("/* pgdog_shard: {shard} */ DROP TABLE IF EXISTS {TEST_TABLE}");
        pool.execute(drop.as_str()).await.unwrap();
        let create = format!(
            "/* pgdog_shard: {shard} */ CREATE TABLE {TEST_TABLE} (id BIGINT PRIMARY KEY, value TEXT)"
        );
        pool.execute(create.as_str()).await.unwrap();
    }
}

async fn cleanup_table(pool: &Pool<Postgres>) {
    for shard in [0, 1] {
        let drop = format!("/* pgdog_shard: {shard} */ DROP TABLE IF EXISTS {TEST_TABLE}");
        pool.execute(drop.as_str()).await.ok();
    }
}

async fn count_on_shard(pool: &Pool<Postgres>, shard: i32, id: i64) -> i64 {
    let sql = format!(
        "/* pgdog_shard: {shard} */ SELECT COUNT(*)::bigint FROM {TEST_TABLE} WHERE id = {id}"
    );
    sqlx::query_scalar(sql.as_str())
        .fetch_one(pool)
        .await
        .unwrap()
}

async fn verify_sharded_table_empty(pool: &Pool<Postgres>) {
    for shard in [0, 1] {
        let count = sqlx::query_scalar::<_, i64>(&format!(
            "/* pgdog_shard: {shard} */ SELECT COUNT(*) FROM {TEST_TABLE}"
        ))
        .fetch_one(pool)
        .await
        .unwrap();
        assert_eq!(count, 0, "expected shard {shard} to remain empty");
    }
}

async fn prepare_non_sharded_table(pool: &Pool<Postgres>) {
    let drop = format!("/* pgdog_shard: 0 */ DROP TABLE IF EXISTS {NON_SHARDED_TABLE}");
    pool.execute(drop.as_str()).await.ok();
    let drop = format!("/* pgdog_shard: 1 */ DROP TABLE IF EXISTS {NON_SHARDED_TABLE}");
    pool.execute(drop.as_str()).await.ok();

    sqlx::query(
        &format!(
            "CREATE TABLE IF NOT EXISTS {NON_SHARDED_TABLE} (id BIGINT PRIMARY KEY, name TEXT)"
        ),
    )
    .execute(pool)
    .await
    .expect("create non-sharded table");

    sqlx::query(&format!("TRUNCATE TABLE {NON_SHARDED_TABLE}")).execute(pool)
        .await
        .expect("truncate non-sharded table");
}

async fn cleanup_non_sharded_table(pool: &Pool<Postgres>) {
    sqlx::query(&format!("DROP TABLE IF EXISTS {NON_SHARDED_TABLE}")).execute(pool)
        .await
        .ok();
}
