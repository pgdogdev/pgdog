use integration_tests_rust::setup::connection_sqlx_direct_db;
use sqlx::Row;

#[tokio::test]
async fn test_postgres_version_in_ci() {
    let direct_postgres_conn = connection_sqlx_direct_db("shard_1").await;

    let pg_version_row = sqlx::raw_sql("SELECT version();")
        .fetch_one(&direct_postgres_conn)
        .await
        .unwrap();

    let version = pg_version_row.get::<&str, &str>("version");
    assert!(version.contains("PostgreSQL 18"));
}
