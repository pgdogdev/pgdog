use crate::setup::{admin_sqlx, connections_sqlx};
use sqlx::postgres::types::Oid;
use sqlx::{Column, Executor, Row};

#[derive(sqlx::Type, Debug, Clone, PartialEq)]
#[sqlx(type_name = "test_oid_drift_composite")]
struct Composite {
    a: String,
    b: String,
}

#[derive(sqlx::Type, Debug, Clone, PartialEq)]
#[sqlx(type_name = "test_oid_drift_later_composite")]
struct LaterComposite {
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

/// A type created after PgDog loaded its OID mappings (here: with DDL reloads
/// disabled) is resolved on first use, so the very first query using it
/// already gets the canonical OID.
#[tokio::test]
async fn test_oid_drift_type_created_later() {
    let conn = connections_sqlx().await.pop().unwrap();
    let admin = admin_sqlx().await;

    admin
        .execute("SET canonicalize_type_information TO true")
        .await
        .unwrap();
    admin
        .execute("SET reload_schema_on_ddl TO false")
        .await
        .unwrap();
    // Make sure the mappings are loaded before the type exists.
    admin.execute("RELOAD").await.unwrap();
    conn.execute("SELECT 1").await.unwrap();

    conn.execute("DROP TABLE IF EXISTS test_oid_drift_later")
        .await
        .unwrap();
    conn.execute("DROP TYPE IF EXISTS test_oid_drift_later_composite CASCADE")
        .await
        .unwrap();
    // Intentionally cause the OID of the type to differ between shards
    conn.execute("/* pgdog_shard: 1 */ CREATE SEQUENCE foo; DROP SEQUENCE foo;")
        .await
        .unwrap();
    conn.execute("CREATE TYPE test_oid_drift_later_composite AS (a text, b text)")
        .await
        .unwrap();
    conn.execute(
        "CREATE TABLE test_oid_drift_later (customer_id BIGINT, composite test_oid_drift_later_composite)",
    )
    .await
    .unwrap();

    let expected_oid: Oid = sqlx::query_scalar(
        "SELECT oid FROM pg_type WHERE typname = 'test_oid_drift_later_composite'",
    )
    .fetch_one(&conn)
    .await
    .unwrap();

    // The client learned the type's OID from shard 0 and sends it in Parse;
    // shards where the OID differs must accept it on the first try.
    let composite = LaterComposite {
        a: String::from("a"),
        b: String::from("b"),
    };
    for i in 1..=20_i64 {
        sqlx::query("INSERT INTO test_oid_drift_later VALUES ($1, $2)")
            .bind(i)
            .bind(&composite)
            .execute(&conn)
            .await
            .unwrap();
    }

    // Reads from every shard, including the ones where the OID differs,
    // must not fail even once.
    for customer_id in 1..=20_i64 {
        let rows = sqlx::query("SELECT composite FROM test_oid_drift_later WHERE customer_id = $1")
            .bind(customer_id)
            .fetch_all(&conn)
            .await
            .unwrap();
        assert_eq!(rows.len(), 1, "customer {customer_id}");
        assert_eq!(
            rows[0].column(0).type_info().oid(),
            Some(expected_oid),
            "customer {customer_id}"
        );
        let decoded: LaterComposite = rows[0].get(0);
        assert_eq!(decoded, composite);
    }

    admin.execute("RELOAD").await.unwrap();
}
