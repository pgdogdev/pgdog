use crate::setup::{admin_tokio, connection_sqlx_direct_db, connections_tokio};
use tokio_postgres::{Client, Error, SimpleQueryMessage};

#[tokio::test]
async fn numeric_infinities_merge_across_shards() -> Result<(), Box<dyn std::error::Error>> {
    let connections = connections_tokio().await;
    let sharded = &connections[1];
    sharded
        .batch_execute("DROP TABLE IF EXISTS numeric_infinity_test")
        .await?;
    sharded
        .batch_execute(
            "CREATE TABLE numeric_infinity_test (customer_id BIGINT, kind TEXT, value NUMERIC)",
        )
        .await?;
    admin_tokio().await.batch_execute("RELOAD").await?;

    sharded
        .batch_execute(
            "/* pgdog_shard: 0 */ INSERT INTO numeric_infinity_test VALUES
             (1, 'negative', '-Infinity'), (2, 'positive', 2.5), (3, 'mixed', '-Infinity')",
        )
        .await?;
    sharded
        .batch_execute(
            "/* pgdog_shard: 1 */ INSERT INTO numeric_infinity_test VALUES
             (101, 'negative', -2.5), (102, 'positive', 'Infinity'), (103, 'mixed', 'Infinity')",
        )
        .await?;

    // Read each backend directly to prove every aggregate group spans both shards.
    for (database, expected) in [
        (
            "shard_0",
            [(1_i64, "-Infinity"), (2, "2.5"), (3, "-Infinity")],
        ),
        (
            "shard_1",
            [(101_i64, "-2.5"), (102, "Infinity"), (103, "Infinity")],
        ),
    ] {
        let backend = connection_sqlx_direct_db(database).await;
        let actual: Vec<(i64, String)> = sqlx::query_as(
            "SELECT customer_id, value::text FROM numeric_infinity_test ORDER BY customer_id",
        )
        .fetch_all(&backend)
        .await?;
        assert_eq!(
            actual,
            expected.map(|(id, value)| (id, value.to_owned())),
            "rows stored on {database}"
        );
        backend.close().await;
    }

    assert_eq!(
        text_rows(
            sharded,
            "SELECT customer_id, value FROM numeric_infinity_test ORDER BY value, customer_id",
        )
        .await?,
        [
            ["1", "-Infinity"],
            ["3", "-Infinity"],
            ["101", "-2.5"],
            ["2", "2.5"],
            ["102", "Infinity"],
            ["103", "Infinity"],
        ]
    );
    assert_eq!(
        text_rows(
            sharded,
            "SELECT value, COUNT(*) FROM numeric_infinity_test GROUP BY value ORDER BY value",
        )
        .await?,
        [
            ["-Infinity", "2"],
            ["-2.5", "1"],
            ["2.5", "1"],
            ["Infinity", "2"]
        ]
    );
    assert_eq!(
        text_rows(
            sharded,
            "SELECT kind, MIN(value), MAX(value), SUM(value), AVG(value)
             FROM numeric_infinity_test GROUP BY kind ORDER BY kind",
        )
        .await?,
        [
            ["mixed", "-Infinity", "Infinity", "NaN", "NaN"],
            ["negative", "-Infinity", "-2.5", "-Infinity", "-Infinity"],
            ["positive", "2.5", "Infinity", "Infinity", "Infinity"],
        ]
    );

    sharded
        .batch_execute("DROP TABLE numeric_infinity_test")
        .await?;
    Ok(())
}

async fn text_rows(client: &Client, query: &str) -> Result<Vec<Vec<String>>, Error> {
    Ok(client
        .simple_query(query)
        .await?
        .into_iter()
        .filter_map(|message| match message {
            SimpleQueryMessage::Row(row) => Some(
                (0..row.len())
                    .map(|column| row.get(column).expect("non-NULL test value").to_owned())
                    .collect(),
            ),
            _ => None,
        })
        .collect())
}
