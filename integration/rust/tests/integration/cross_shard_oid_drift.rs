#![cfg(feature = "new_parser")]
use crate::setup::{admin_sqlx, admin_tokio, connections_sqlx, connections_tokio};
use sqlx::Executor;
use tokio_postgres::SimpleQueryMessage;

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
    assert_eq!(rows, vec![composite; 20]);
}

#[tokio::test]
async fn test_oid_drift_simple_protocol() {
    let conn = connections_tokio().await.pop().unwrap();

    // Intentionally cause the OID of the type to differ between shards
    conn.simple_query("/* pgdog_shard: 0 */ CREATE SEQUENCE foo; DROP SEQUENCE foo;")
        .await
        .unwrap();
    conn.simple_query("DROP TYPE IF EXISTS test_oid_drift_simple_composite CASCADE")
        .await
        .unwrap();
    conn.simple_query("CREATE TYPE test_oid_drift_simple_composite AS (a text, b text)")
        .await
        .unwrap();
    conn.simple_query("DROP TABLE IF EXISTS test_oid_drift_simple")
        .await
        .unwrap();
    conn.simple_query(
        "CREATE TABLE test_oid_drift_simple (customer_id BIGINT, composite test_oid_drift_simple_composite)",
    )
    .await
    .unwrap();
    admin_tokio().await.simple_query("RELOAD").await.unwrap();

    let composite = Composite {
        a: String::from("b"),
        b: String::from("c"),
    };
    for i in 1..=20 {
        conn.simple_query(&format!(
                "INSERT INTO test_oid_drift_simple VALUES ({}, ROW('{}', '{}')::test_oid_drift_simple_composite)",
                i,
                composite.a,
                composite.b,
            ))
            .await
            .unwrap();
    }

    let messages = conn
        .simple_query("SELECT composite FROM test_oid_drift_simple")
        .await
        .unwrap();
    assert_eq!(messages.len(), 22);
    let rows = messages
        .iter()
        .skip(1) // RowDescription
        .take(20) // CommandComplete at end
        .map(|message| {
            let SimpleQueryMessage::Row(row) = message else {
                panic!("not a DataRow: {:?}", message);
            };
            row.get(0).unwrap()
        })
        .collect::<Vec<_>>();
    assert_eq!(rows, vec!["(b,c)"; 20]);
}
