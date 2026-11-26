use super::numeric_infinity::text_rows;
use crate::setup::{admin_tokio, connections_sqlx, connections_tokio};
use sqlx::{Executor, Row};

#[tokio::test]
async fn sorts_by_case_expression_across_shards() {
    let connections = connections_sqlx().await;
    let sharded_connection = &connections[1];

    sharded_connection
        .execute("TRUNCATE sharded")
        .await
        .unwrap();

    for (id, value) in [
        (11, "free"),
        (12, "pay"),
        (13, "free"),
        (14, "pay"),
        (15, "free"),
        (16, "pay"),
    ] {
        sharded_connection
            .execute(unsafe_sql!(
                "INSERT INTO sharded (id, value) VALUES ({id}, '{value}')"
            ))
            .await
            .unwrap();
    }

    for sort_query in [
        "SELECT id, value FROM sharded ORDER BY CASE value WHEN 'pay' THEN 0 ELSE 1 END, id",
        "SELECT id, value, CASE value WHEN 'pay' THEN 0 ELSE 1 END AS prio FROM sharded ORDER BY 3, 1",
        "SELECT id, value, CASE value WHEN 'pay' THEN 0 ELSE 1 END AS prio FROM sharded ORDER BY CASE value WHEN 'pay' THEN 0 ELSE 1 END, id",
        "SELECT id, value, CASE value WHEN 'pay' THEN 0 ELSE 1 END AS prio FROM sharded ORDER BY prio, id",
    ] {
        let sorted_ids: Vec<i64> = sqlx::query(sort_query)
            .fetch_all(sharded_connection)
            .await
            .unwrap()
            .iter()
            .map(|sorted_row| sorted_row.get(0))
            .collect();

        assert_eq!(sorted_ids, [12, 14, 16, 11, 13, 15], "{sort_query}");
    }
}

#[tokio::test]
async fn rejects_count_sort_key_and_keeps_connection_open() {
    let connections = connections_sqlx().await;
    let sharded_connection = &connections[1];

    let rejection_error = sharded_connection
        .execute("SELECT value FROM sharded GROUP BY value ORDER BY count(*)")
        .await
        .unwrap_err();

    let database_error = rejection_error.as_database_error().unwrap();

    assert_eq!(
        database_error.message(),
        "ORDER BY an expression with an aggregate or DISTINCT is not supported across shards"
    );
    assert_eq!(database_error.code().as_deref(), Some("0A000"));

    let (still_connected,): (i32,) = sqlx::query_as("SELECT 1")
        .fetch_one(sharded_connection)
        .await
        .unwrap();

    assert_eq!(still_connected, 1);
}

/// `ORDER BY count(*)` sorts by the merged SELECT-list count, not by one
/// shard's partial count (ClickBench Q08).
#[tokio::test]
async fn sorts_by_selected_aggregate_across_shards() -> Result<(), Box<dyn std::error::Error>> {
    let connections = connections_tokio().await;
    let sharded = &connections[1];
    sharded
        .batch_execute("DROP TABLE IF EXISTS order_by_count_test")
        .await?;
    sharded
        .batch_execute("CREATE TABLE order_by_count_test (customer_id BIGINT, kind TEXT)")
        .await?;
    admin_tokio().await.batch_execute("RELOAD").await?;

    // Shard 0 alone ranks 'a' first; across both shards 'b' has more rows.
    sharded
        .batch_execute(
            "/* pgdog_shard: 0 */ INSERT INTO order_by_count_test VALUES
             (1, 'a'), (2, 'a'), (3, 'a'), (4, 'b')",
        )
        .await?;
    sharded
        .batch_execute(
            "/* pgdog_shard: 1 */ INSERT INTO order_by_count_test VALUES
             (101, 'b'), (102, 'b'), (103, 'b'), (104, 'c')",
        )
        .await?;

    for query in [
        "SELECT kind, COUNT(*) FROM order_by_count_test GROUP BY kind ORDER BY COUNT(*) DESC",
        "SELECT kind, count(*) AS c FROM order_by_count_test GROUP BY kind ORDER BY count(*) DESC",
    ] {
        assert_eq!(
            text_rows(sharded, query).await?,
            [["b", "4"], ["a", "3"], ["c", "1"]],
            "{query}"
        );
    }

    sharded
        .batch_execute("DROP TABLE order_by_count_test")
        .await?;
    Ok(())
}
