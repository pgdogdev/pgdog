use rust::setup::connections_sqlx;
use sqlx::{Executor, Row};

#[tokio::test]
async fn unique_id_returns_bigint() -> Result<(), Box<dyn std::error::Error>> {
    let conns = connections_sqlx().await;
    let sharded = conns.get(1).cloned().unwrap();

    // Simple query
    let row = sharded.fetch_one("SELECT pgdog.unique_id() AS id").await?;
    let mut id: i64 = row.get("id");

    assert!(
        id > 0,
        "unique_id should return a positive bigint, got {id}"
    );

    for _ in 0..100 {
        // Prepared statement
        let row = sqlx::query("SELECT pgdog.unique_id() AS id")
            .fetch_one(&sharded)
            .await?;
        let prepared_id: i64 = row.get("id");
        assert!(
            prepared_id > 0,
            "prepared unique_id should return a positive bigint, got {prepared_id}"
        );

        assert!(
            prepared_id > id,
            "prepared id should be greater than simple query id"
        );
        id = prepared_id;
    }

    Ok(())
}
