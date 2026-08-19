//! Regression test for <https://github.com/pgdogdev/pgdog/issues/1374>

use integration_tests_rust::setup::admin_sqlx;
use sqlx::{Connection, Executor, Row};

// None of these should throw errors.
#[tokio::test]
async fn test_cross_shard_omni_check_valid_cases() {
    let admin = admin_sqlx().await;
    admin
        .execute("SET read_write_strategy TO conservative")
        .await
        .unwrap();

    let mut conn =
        sqlx::PgConnection::connect("postgres://pgdog:pgdog@127.0.0.1:6432/pgdog_schema")
            .await
            .unwrap();

    // Test without using search_path. Just pgdog.shard set.
    // The expected behavior is that it will not get an omnisharded write error solely because we have
    // schema sharding configured and are *NOT* using table sharding.
    {
        let mut transaction = conn.begin().await.unwrap();
        transaction
            .execute("SET LOCAL pgdog.shard TO 0")
            .await
            .unwrap();

        let result = transaction
            .fetch_one(
                "
                    SELECT
                        typname AS name, oid, typarray AS array_oid,
                        oid::regtype::text AS regtype, typdelim AS delimiter
                    FROM pg_type t
                    WHERE t.oid = to_regtype('int4')
                    ORDER BY t.oid;",
            )
            .await
            .unwrap();

        assert_eq!(result.get::<&str, &str>("name"), "int4");
        transaction.commit().await.unwrap();
    }

    // Test without using pgdog.shard; just use search_path.
    // Nothing is abnormal about this (vs the last one); it should work fine.
    {
        let mut transaction = conn.begin().await.unwrap();
        transaction
            .execute("SET LOCAL search_path TO shard_0")
            .await
            .unwrap();

        let result = transaction
            .fetch_one(
                "
                    SELECT
                        typname AS name, oid, typarray AS array_oid,
                        oid::regtype::text AS regtype, typdelim AS delimiter
                    FROM pg_type t
                    WHERE t.oid = to_regtype('int4')
                    ORDER BY t.oid;",
            )
            .await
            .unwrap();

        assert_eq!(result.get::<&str, &str>("name"), "int4");

        transaction.commit().await.unwrap();
    }

    // Test using both pgdog.shard and search_path.
    // pgdog.shard has priority being a `Set` over `SearchPath`
    // The expected behavior is that it will not get an omnisharded write error solely because we have
    // schema sharding configured and are *NOT* using table sharding.
    {
        let mut transaction = conn.begin().await.unwrap();
        transaction
            .execute("SET LOCAL search_path TO shard_0")
            .await
            .unwrap();
        transaction
            .execute("SET LOCAL pgdog.shard TO 0")
            .await
            .unwrap();

        let result = transaction
            .fetch_one(
                "
                    SELECT
                        typname AS name, oid, typarray AS array_oid,
                        oid::regtype::text AS regtype, typdelim AS delimiter
                    FROM pg_type t
                    WHERE t.oid = to_regtype('int4')
                    ORDER BY t.oid;",
            )
            .await
            .unwrap();

        assert_eq!(result.get::<&str, &str>("name"), "int4");

        transaction.commit().await.unwrap();
    }

    // Test using both pgdog.shard and search_path when they disagree on shard mapping.
    // pgdog.shard has priority being a `Set` over `SearchPath`
    // The expected behavior is that it will not get an omnisharded write error solely because we have
    // schema sharding configured and are *NOT* using table sharding.
    // Even though they disagree on a shard mapping, we expect people using SET to know what they're doing,
    // and the query should work fine.
    {
        let mut transaction = conn.begin().await.unwrap();
        transaction
            .execute("SET LOCAL search_path TO shard_0")
            .await
            .unwrap();
        transaction
            .execute("SET LOCAL pgdog.shard TO 1")
            .await
            .unwrap();

        let result = transaction
            .fetch_one(
                "
                    SELECT
                        typname AS name, oid, typarray AS array_oid,
                        oid::regtype::text AS regtype, typdelim AS delimiter
                    FROM pg_type t
                    WHERE t.oid = to_regtype('int4')
                    ORDER BY t.oid;",
            )
            .await
            .unwrap();

        assert_eq!(result.get::<&str, &str>("name"), "int4");

        transaction.commit().await.unwrap();
    }
}

// All of these should throw errors.
#[tokio::test]
async fn test_cross_shard_omni_check_invalid_cases() {
    let admin = admin_sqlx().await;
    admin
        .execute("SET read_write_strategy TO conservative")
        .await
        .unwrap();

    let mut conn =
        sqlx::PgConnection::connect("postgres://pgdog:pgdog@127.0.0.1:6432/pgdog_sharded")
            .await
            .unwrap();

    // Test on a table that's NOT schema sharded.
    // Try a direct-to-shard SET in a transaction using a SELECT to an omnisharded table.
    // Should fail with "cannot write to an omnisharded table with a shard directive"
    {
        let mut transaction = conn.begin().await.unwrap();
        transaction
            .execute("SET LOCAL pgdog.shard TO 0")
            .await
            .unwrap();

        let error = transaction
            .fetch_one(
                "
                    SELECT
                        typname AS name, oid, typarray AS array_oid,
                        oid::regtype::text AS regtype, typdelim AS delimiter
                    FROM pg_type t
                    WHERE t.oid = to_regtype('int4')
                    ORDER BY t.oid;",
            )
            .await
            .err()
            .unwrap();

        assert!(
            error
                .to_string()
                .contains("cannot write to an omnisharded table with a shard directive")
        );
        transaction.commit().await.unwrap();
    }
}
