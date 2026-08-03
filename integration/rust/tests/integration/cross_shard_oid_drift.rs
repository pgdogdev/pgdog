#![cfg(feature = "new_parser")]
use crate::setup::{admin_sqlx, connections_sqlx};
use sqlx::postgres::types::Oid;
use sqlx::{Column, Executor, Row};

#[derive(sqlx::Type, Debug, Clone, PartialEq)]
#[sqlx(type_name = "test_oid_drift_composite")]
struct Composite {
    a: String,
    b: String,
}

#[tokio::test]
async fn test_oid_drift() {
    let conn = connections_sqlx().await.pop().unwrap();
    let admin = admin_sqlx().await;

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
    admin
        .execute("SET canonicalize_type_information TO true")
        .await
        .unwrap();

    let composite = Composite {
        a: String::from("a"),
        b: String::from("b"),
    };
    for i in 1..=20 {
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
    assert_eq!(rows, vec![composite.clone(); 20]);

    let simple_rows = conn
        .fetch_all("SELECT composite FROM test_oid_drift")
        .await
        .unwrap();

    let expected_oid: Oid =
        sqlx::query_scalar("SELECT oid FROM pg_type WHERE typname = 'test_oid_drift_composite'")
            .fetch_one(&conn)
            .await
            .unwrap();
    let given_oid = simple_rows.first().unwrap().column(0).type_info().oid();
    assert_eq!(given_oid, Some(expected_oid));

    let simple_data: Vec<Composite> = simple_rows.into_iter().map(|row| row.get(0)).collect();
    assert_eq!(simple_data, vec![composite; 20]);

    admin.execute("RELOAD").await.unwrap();
}
