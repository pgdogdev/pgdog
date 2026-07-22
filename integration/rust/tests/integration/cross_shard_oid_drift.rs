use crate::setup::{admin_sqlx, connections_sqlx};
use sqlx::Executor;

#[derive(sqlx::Type, Debug, Clone, PartialEq)]
#[sqlx(type_name = "test_oid_drift_composite")]
struct Composite {
    a: String,
    b: String,
}

#[tokio::test]
async fn test_oid_drift() {
    let conn = connections_sqlx().await.pop().unwrap();

    // Intentionally cause the OID of the type to differ between shards
    conn.execute("/* pgdog_shard: 0 */ CREATE SEQUENCE foo; DROP SEQUENCE foo;")
        .await
        .unwrap();
    conn.execute("DROP TYPE IF EXISTS test_oid_drift_composite CASCADE")
        .await
        .unwrap();
    conn.execute("CREATE TYPE test_oid_drift_composite AS (a text, b text)")
        .await
        .unwrap();
    conn.execute("DROP TABLE IF EXISTS test_oid_drift")
        .await
        .unwrap();
    conn.execute(
        "CREATE TABLE test_oid_drift (customer_id BIGINT, composite test_oid_drift_composite)",
    )
    .await
    .unwrap();
    admin_sqlx().await.execute("RELOAD").await.unwrap();

    let composite = Composite {
        a: String::from("a"),
        b: String::from("b"),
    };
    for i in 1..=11 {
        sqlx::query("INSERT INTO test_oid_drift VALUES ($1, $2)")
            .bind(i)
            .bind(&composite)
            .execute(&conn)
            .await
            .unwrap();
    }

    let rows: Vec<Composite> = sqlx::query_scalar("SELECT composite FROM test_oid_drift")
        .fetch_all(&conn)
        .await
        .unwrap();
    assert_eq!(rows, vec![composite; 11]);
}
