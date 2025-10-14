use rust::setup::{admin_sqlx, connections_sqlx};
use sqlx::{Acquire, Executor, Pool, Postgres};

const TEST_TABLE: &str = "sharded_list";

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
