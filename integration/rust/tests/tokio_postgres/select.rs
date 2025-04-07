use rust::setup::connections_tokio;

#[tokio::test]
async fn select_one() {
    for conn in connections_tokio().await {
        for _ in 0..25 {
            let rows = conn.query("SELECT $1::bigint", &[&1_i64]).await.unwrap();

            assert_eq!(rows.len(), 1);
            let one: i64 = rows[0].get(0);
            assert_eq!(one, 1);
        }
    }
}

#[tokio::test]
async fn test_insert() {
    for conn in connections_tokio().await {
        conn.batch_execute(
            "DROP SCHEMA IF EXISTS rust_test_insert CASCADE;
            CREATE SCHEMA rust_test_insert;
            CREATE TABLE rust_test_insert.sharded (id BIGINT PRIMARY KEY, value VARCHAR);",
        )
        .await
        .unwrap();

        for _ in 0..25 {
            let rows = conn
                .query("SELECT * FROM rust_test_insert.sharded", &[])
                .await
                .unwrap();
            assert_eq!(rows.len(), 0);

            let results = conn
                .query(
                    "INSERT INTO rust_test_insert.sharded (id, value) VALUES ($1, $2) RETURNING *",
                    &[&1_i64, &"test"],
                )
                .await
                .unwrap();
            assert_eq!(results.len(), 1);

            conn.execute("DELETE FROM rust_test_insert.sharded", &[])
                .await
                .unwrap();
        }

        conn.execute("DROP SCHEMA IF EXISTS rust_test_insert CASCADE", &[])
            .await
            .unwrap();
    }
}
