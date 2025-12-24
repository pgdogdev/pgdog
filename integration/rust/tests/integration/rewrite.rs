use rust::setup::{admin_sqlx, connections_sqlx};
use sqlx::{Executor, Pool, Postgres};

const TEST_TABLE: &str = "sharded_list";
const SHARDED_INSERT_TABLE: &str = "sharded";
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
            .execute("SET rewrite_enabled TO true")
            .await
            .expect("enable rewrite features");
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
            let _ = admin.execute("SET rewrite_split_inserts TO error").await;
            let _ = admin.execute("SET rewrite_enabled TO false").await;
            let _ = admin.execute("SET two_phase_commit TO false").await;
        });
    }
}

#[tokio::test]
#[ignore]
async fn sharded_multi_row_insert_rejected() {
    let admin = admin_sqlx().await;
    let _guard = RewriteConfigGuard::enable(admin.clone()).await;

    let mut pools = connections_sqlx().await;
    let sharded = pools.swap_remove(1);

    let err = sqlx::query(&format!(
        "INSERT INTO {SHARDED_INSERT_TABLE} (id, value) VALUES (1, 'one'), (2, 'two')"
    ))
    .execute(&sharded)
    .await
    .expect_err("multi-row insert into sharded table should fail for now");

    let db_err = err
        .as_database_error()
        .expect("expected database error from proxy");
    assert_eq!(
        db_err.message(),
        format!(
            "multi-row INSERT into sharded table \"{SHARDED_INSERT_TABLE}\" is not supported when rewrite.split_inserts=error"
        )
    );
}

#[tokio::test]
async fn non_sharded_multi_row_insert_allowed() {
    let admin = admin_sqlx().await;
    let _guard = RewriteConfigGuard::enable(admin.clone()).await;

    let mut pools = connections_sqlx().await;
    let primary = pools.swap_remove(0);

    prepare_non_sharded_table(&primary).await;

    sqlx::query(&format!(
        "INSERT INTO {NON_SHARDED_TABLE} (id, name) VALUES (1, 'one'), (2, 'two')"
    ))
    .execute(&primary)
    .await
    .expect("multi-row insert should succeed for non-sharded table");

    let rows: Vec<(i64, String)> = sqlx::query_as(&format!(
        "SELECT id, name FROM {NON_SHARDED_TABLE} ORDER BY id"
    ))
    .fetch_all(&primary)
    .await
    .expect("read back non-sharded rows");
    assert_eq!(rows.len(), 2, "expected two rows in non-sharded table");
    assert_eq!(rows[0], (1, "one".to_string()));
    assert_eq!(rows[1], (2, "two".to_string()));

    cleanup_non_sharded_table(&primary).await;
}

#[tokio::test]
async fn split_inserts_rewrite_moves_rows_across_shards() {
    let admin = admin_sqlx().await;
    let _guard = RewriteConfigGuard::enable(admin.clone()).await;

    admin
        .execute("SET rewrite_split_inserts TO rewrite")
        .await
        .expect("enable split insert rewrite");

    let mut pools = connections_sqlx().await;
    let pool = pools.swap_remove(1);

    prepare_split_table(&pool).await;

    sqlx::query(&format!(
        "INSERT INTO {SHARDED_INSERT_TABLE} (id, value) VALUES (1, 'one'), (11, 'eleven')"
    ))
    .execute(&pool)
    .await
    .expect("split insert should succeed");

    let shard0: Option<String> = sqlx::query_scalar(&format!(
        "SELECT value FROM {SHARDED_INSERT_TABLE} WHERE id = 1"
    ))
    .fetch_optional(&pool)
    .await
    .expect("fetch shard 0 row");
    let shard1: Option<String> = sqlx::query_scalar(&format!(
        "SELECT value FROM {SHARDED_INSERT_TABLE} WHERE id = 11"
    ))
    .fetch_optional(&pool)
    .await
    .expect("fetch shard 1 row");

    assert_eq!(shard0.as_deref(), Some("one"), "expected row on shard 0");
    assert_eq!(shard1.as_deref(), Some("eleven"), "expected row on shard 1");

    cleanup_split_table(&pool).await;
}

#[tokio::test]
async fn update_moves_row_between_shards() {
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

    let mut txn = pool.begin().await.unwrap();
    let update = format!("UPDATE {TEST_TABLE} SET id = 11 WHERE id = 1");
    let result = txn.execute(update.as_str()).await.expect("rewrite update");
    assert_eq!(result.rows_affected(), 1, "exactly one row updated");
    txn.commit().await.unwrap();

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
async fn update_rejects_multiple_rows() {
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

    let mut txn = pool.begin().await.unwrap();

    let update = format!("UPDATE {TEST_TABLE} SET id = 11 WHERE id IN (1, 2)");
    let err = txn
        .execute(update.as_str())
        .await
        .expect_err("expected multi-row rewrite to fail");
    let db_err = err
        .as_database_error()
        .expect("expected database error from proxy");
    assert!(
        db_err
            .message()
            .contains("sharding key update changes more than one row (2)"),
        "unexpected error message: {}",
        db_err.message()
    );
    txn.rollback().await.unwrap();

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
async fn update_expects_transactions() {
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

    let update = format!("UPDATE {TEST_TABLE} SET id = 11 WHERE id = 1");
    let err = conn
        .execute(update.as_str())
        .await
        .expect_err("sharding key update must be executed inside a transaction");
    let db_err = err
        .as_database_error()
        .expect("expected database error from proxy");
    assert!(
        db_err
            .message()
            .contains("sharding key update must be executed inside a transaction"),
        "unexpected error message: {}",
        db_err.message()
    );

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

async fn prepare_split_table(pool: &Pool<Postgres>) {
    for shard in [0, 1] {
        let drop =
            format!("/* pgdog_shard: {shard} */ DROP TABLE IF EXISTS {SHARDED_INSERT_TABLE}");
        pool.execute(drop.as_str()).await.unwrap();
        let create = format!(
            "/* pgdog_shard: {shard} */ CREATE TABLE {SHARDED_INSERT_TABLE} (id BIGINT PRIMARY KEY, value TEXT)"
        );
        pool.execute(create.as_str()).await.unwrap();
    }
}

async fn cleanup_split_table(pool: &Pool<Postgres>) {
    for shard in [0, 1] {
        let drop =
            format!("/* pgdog_shard: {shard} */ DROP TABLE IF EXISTS {SHARDED_INSERT_TABLE}");
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

async fn prepare_non_sharded_table(pool: &Pool<Postgres>) {
    let _ = sqlx::query(&format!("DROP TABLE IF EXISTS {NON_SHARDED_TABLE}"))
        .execute(pool)
        .await;

    sqlx::query(&format!(
        "CREATE TABLE IF NOT EXISTS {NON_SHARDED_TABLE} (id BIGINT PRIMARY KEY, name TEXT)"
    ))
    .execute(pool)
    .await
    .expect("create non-sharded table");

    sqlx::query(&format!("TRUNCATE TABLE {NON_SHARDED_TABLE}"))
        .execute(pool)
        .await
        .expect("truncate non-sharded table");
}

async fn cleanup_non_sharded_table(pool: &Pool<Postgres>) {
    let _ = sqlx::query(&format!("DROP TABLE IF EXISTS {NON_SHARDED_TABLE}"))
        .execute(pool)
        .await;
}
