//! Test cluster with just one sharding function.

use rust::setup::connections_sqlx;
use sqlx::Connection;
use sqlx::prelude::*;

#[tokio::test]
async fn test_single_sharding_function() {
    let mut conn =
        sqlx::PgConnection::connect("postgres://pgdog:pgdog@127.0.0.1:6432/single_sharded_list")
            .await
            .unwrap();

    conn.execute("DROP TABLE IF EXISTS test_single_sharding_function")
        .await
        .unwrap();
    conn.execute(
        "CREATE TABLE IF NOT EXISTS test_single_sharding_function(id BIGINT, value VARCHAR)",
    )
    .await
    .unwrap();

    for id in 1..21 {
        let result =
            sqlx::query("INSERT INTO test_single_sharding_function (id, value) VALUES ($1, $2)")
                .bind(id as i64)
                .bind(format!("value_{id}"))
                .execute(&mut conn)
                .await
                .unwrap();
        assert_eq!(result.rows_affected(), 1);
    }

    for id in 1..21 {
        conn.execute("BEGIN").await.unwrap();
        conn.execute(format!("SET pgdog.sharding_key TO '{id}'").as_str())
            .await
            .unwrap();
        let result: (i64,) =
            sqlx::query_as("SELECT COUNT(*)::bigint FROM test_single_sharding_function")
                .fetch_one(&mut conn)
                .await
                .unwrap();
        assert_eq!(result.0, 10);
        conn.execute("COMMIT").await.unwrap();
    }

    conn.execute("DROP TABLE test_single_sharding_function")
        .await
        .unwrap();
    conn.close().await.unwrap();
}

#[tokio::test]
async fn test_single_sharding_function_rejected() {
    let mut conns = connections_sqlx().await;
    {
        let sharded = &mut conns[1];

        sharded.execute("BEGIN").await.unwrap();
        let result = sharded.execute("SET pgdog.sharding_key TO '1'").await;
        assert!(
            result
                .err()
                .unwrap()
                .to_string()
                .contains("config has more than one sharding function")
        );
    }

    {
        let normal = &mut conns[0];
        normal.execute("BEGIN").await.unwrap();
        let _ = normal
            .execute("SET pgdog.sharding_key TO '1'")
            .await
            .unwrap();
    }
}

#[tokio::test]
async fn test_set_require_begin() {
    let conns = connections_sqlx().await;

    for conn in conns {
        for query in ["SET pgdog.sharding_key TO '1'", "SET pgdog.shard TO 0"] {
            let result = conn.execute(query).await.err().unwrap();
            assert!(
                result
                    .to_string()
                    .contains("this command requires a transaction")
            );
        }
    }
}
