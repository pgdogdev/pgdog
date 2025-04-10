use rust::setup::connections_sqlx;
use tokio::task::JoinSet;

#[tokio::test]
async fn test_connect() {
    for conn in connections_sqlx().await {
        for i in 0..1 {
            let row: (i64,) = sqlx::query_as("SELECT $1")
                .bind(i)
                .fetch_one(&conn)
                .await
                .unwrap();

            assert_eq!(row.0, i);
        }

        conn.close().await;
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn test_concurrent() {
    let mut tasks = JoinSet::new();

    for conn in connections_sqlx().await {
        sqlx::query("CREATE SCHEMA IF NOT EXISTS rust_test_concurrent")
            .execute(&conn)
            .await
            .unwrap();

        sqlx::query("CREATE TABLE IF NOT EXISTS rust_test_concurrent.sharded (id BIGINT PRIMARY KEY, value TEXT)")
            .execute(&conn)
            .await
            .unwrap();

        sqlx::query("TRUNCATE TABLE rust_test_concurrent.sharded")
            .execute(&conn)
            .await
            .unwrap();
    }

    for i in 0..25 {
        tasks.spawn(async move {
            for conn in connections_sqlx().await {
                for id in i * 25..i * 25 + 100 {
                    let row: Option<(i64,)> =
                        sqlx::query_as("SELECT * FROM rust_test_concurrent.sharded WHERE id = $1")
                            .bind(id)
                            .fetch_optional(&conn)
                            .await
                            .unwrap();
                    assert!(row.is_none());

                    let rows: Vec<(i64, String)> = sqlx::query_as(
                        "INSERT INTO rust_test_concurrent.sharded (id, value) VALUES ($1, $2) RETURNING *",
                    )
                    .bind(id)
                    .bind(format!("value_{}", id))
                    .fetch_all(&conn)
                    .await
                    .unwrap();
                    assert_eq!(rows.len(), 1);
                    assert_eq!(rows[0].0, id);
                    assert_eq!(rows[0].1, format!("value_{}", id));

                    sqlx::query("DELETE FROM rust_test_concurrent.sharded WHERE id = $1")
                        .bind(id)
                        .execute(&conn)
                        .await
                        .unwrap();
                }
            }
        });
    }

    tasks.join_all().await;

    for conn in connections_sqlx().await {
        let count: (i64,) =
            sqlx::query_as("SELECT COUNT(*)::bigint FROM rust_test_concurrent.sharded")
                .fetch_one(&conn)
                .await
                .unwrap();
        assert_eq!(count.0, 0);

        sqlx::query("DROP SCHEMA rust_test_concurrent CASCADE")
            .execute(&conn)
            .await
            .unwrap();
    }
}
