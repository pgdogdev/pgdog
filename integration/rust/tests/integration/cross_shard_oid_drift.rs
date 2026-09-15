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

#[derive(sqlx::Type, Debug, Clone, Copy, PartialEq)]
#[sqlx(type_name = "test_oid_drift_mood", rename_all = "lowercase")]
enum Mood {
    Sad,
    Ok,
    Happy,
}

/// Binary arrays embed the element type's OID, so arrays of custom types
/// have to be rewritten in both directions, not just the RowDescription.
#[tokio::test]
async fn test_oid_drift_arrays() {
    let conn = connections_sqlx().await.pop().unwrap();
    let admin = admin_sqlx().await;

    conn.execute("DROP TABLE IF EXISTS test_oid_drift_arrays")
        .await
        .unwrap();
    conn.execute("DROP TYPE IF EXISTS test_oid_drift_mood CASCADE")
        .await
        .unwrap();
    // Intentionally cause the OID of the type to differ between shards
    conn.execute("/* pgdog_shard: 1 */ CREATE SEQUENCE foo; DROP SEQUENCE foo;")
        .await
        .unwrap();
    conn.execute("CREATE TYPE test_oid_drift_mood AS ENUM ('sad', 'ok', 'happy')")
        .await
        .unwrap();
    conn.execute(
        "CREATE TABLE test_oid_drift_arrays (customer_id BIGINT, moods test_oid_drift_mood[])",
    )
    .await
    .unwrap();
    admin
        .execute("SET canonicalize_type_information TO true")
        .await
        .unwrap();
    admin.execute("RELOAD").await.unwrap();

    let canonical_oid: Oid =
        sqlx::query_scalar("SELECT oid FROM pg_type WHERE typname = 'test_oid_drift_mood'")
            .fetch_one(&conn)
            .await
            .unwrap();

    let moods = vec![Mood::Sad, Mood::Ok, Mood::Happy];
    for i in 1..=20_i64 {
        // Binary array parameter, element OID as learned from shard 0.
        sqlx::query("INSERT INTO test_oid_drift_arrays VALUES ($1, $2)")
            .bind(i)
            .bind(&moods)
            .execute(&conn)
            .await
            .unwrap();
    }

    for customer_id in 1..=20_i64 {
        let row = sqlx::query("SELECT moods FROM test_oid_drift_arrays WHERE customer_id = $1")
            .bind(customer_id)
            .fetch_one(&conn)
            .await
            .unwrap();

        // The element OID inside the binary array payload is shard 0's.
        let raw = row.try_get_raw(0).unwrap().as_bytes().unwrap().to_vec();
        let element_oid = u32::from_be_bytes([raw[8], raw[9], raw[10], raw[11]]);
        assert_eq!(element_oid, canonical_oid.0, "customer {customer_id}");

        let decoded: Vec<Mood> = row.get(0);
        assert_eq!(decoded, moods);
    }

    admin.execute("RELOAD").await.unwrap();
}
