use rust::setup::connections_tokio;

#[tokio::test]
async fn test_nulls() {
    let conns = connections_tokio().await;

    for conn in conns {
        conn.batch_execute(
            "CREATE SCHEMA IF NOT EXISTS test_nulls;
            CREATE TABLE IF NOT EXISTS test_nulls.sharded (id BIGINT PRIMARY KEY, value TEXT);
            TRUNCATE TABLE test_nulls.sharded;",
        )
        .await
        .unwrap();

        let results = conn
            .query(
                "INSERT INTO test_nulls.sharded (id, value) VALUES ($1, $2) RETURNING *",
                &[&1_i64, &None::<String>],
            )
            .await
            .unwrap();

        assert_eq!(results.len(), 1);
        let row = results.first().unwrap();
        let id: i64 = row.get(0);
        let value: Option<String> = row.get(1);
        assert_eq!(id, 1_i64);
        assert_eq!(value, None);
    }
}
