use rust::setup::connections_tokio;
use tokio_postgres::SimpleQueryMessage;

#[tokio::test]
async fn explain_routing_annotations_surface() -> Result<(), Box<dyn std::error::Error>> {
    let mut clients = connections_tokio().await;
    let sharded = clients.swap_remove(1);

    for shard in [0, 1] {
        let drop = format!(
            "/* pgdog_shard: {} */ DROP TABLE IF EXISTS explain_avg_test",
            shard
        );
        sharded.simple_query(drop.as_str()).await.ok();
    }

    for shard in [0, 1] {
        let create = format!(
            "/* pgdog_shard: {} */ CREATE TABLE explain_avg_test(price DOUBLE PRECISION)",
            shard
        );
        sharded.simple_query(create.as_str()).await?;
    }

    sharded
        .simple_query(
            "/* pgdog_shard: 0 */ INSERT INTO explain_avg_test(price) VALUES (10.0), (14.0)",
        )
        .await?;
    sharded
        .simple_query(
            "/* pgdog_shard: 1 */ INSERT INTO explain_avg_test(price) VALUES (18.0), (22.0)",
        )
        .await?;

    let rows = sharded
        .simple_query("EXPLAIN SELECT AVG(price) FROM explain_avg_test")
        .await?;

    let mut plan_lines = vec![];
    for message in rows {
        if let SimpleQueryMessage::Row(row) = message {
            plan_lines.push(row.get(0).unwrap_or_default().to_string());
        }
    }

    assert!(
        plan_lines.iter().any(|line| line.contains("Aggregate")),
        "missing Aggregate node in EXPLAIN output: {:?}",
        plan_lines
    );
    let routing_header = plan_lines
        .iter()
        .find(|line| line.contains("PgDog Routing:"));
    assert!(
        routing_header.is_some(),
        "missing PgDog Routing header in EXPLAIN output: {:?}",
        plan_lines
    );
    let summary_line = plan_lines
        .iter()
        .find(|line| line.contains("Summary:"))
        .cloned();
    assert!(
        summary_line
            .as_ref()
            .map(|line| line.contains("Summary: shard=all"))
            .unwrap_or(false),
        "unexpected summary line: {:?} (all lines: {:?})",
        summary_line,
        plan_lines
    );
    let broadcast_line = plan_lines
        .iter()
        .find(|line| line.contains("no sharding key matched"))
        .cloned();
    assert!(
        broadcast_line.is_some(),
        "missing broadcast note in EXPLAIN output: {:?}",
        plan_lines
    );

    for shard in [0, 1] {
        let drop = format!(
            "/* pgdog_shard: {} */ DROP TABLE IF EXISTS explain_avg_test",
            shard
        );
        sharded.simple_query(drop.as_str()).await.ok();
    }

    Ok(())
}
