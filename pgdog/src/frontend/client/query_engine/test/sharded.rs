use crate::{
    expect_message,
    net::{
        CommandComplete, DataRow, ErrorResponse, Parameters, Query, ReadyForQuery, RowDescription,
    },
};

use rust_decimal::Decimal;

use super::prelude::*;

#[tokio::test]
async fn test_sharded_insert_sums_row_counts() {
    let mut client = TestClient::new_sharded(Parameters::default()).await;

    let id_shard0 = client.random_id_for_shard(0);
    let id_shard1 = client.random_id_for_shard(1);

    // Cleanup first
    client
        .send_simple(Query::new(format!(
            "DELETE FROM sharded WHERE id IN ({}, {})",
            id_shard0, id_shard1
        )))
        .await;
    client.read_until('Z').await.unwrap();

    // Multi-row INSERT with rows going to different shards - should be split and summed
    client
        .send_simple(Query::new(format!(
            "INSERT INTO sharded (id, value) VALUES ({}, 'ins1'), ({}, 'ins2')",
            id_shard0, id_shard1
        )))
        .await;

    let cc = expect_message!(client.read().await, CommandComplete);
    assert_eq!(
        cc.command(),
        "INSERT 0 2",
        "sharded INSERT should return summed row count from all shards"
    );
    expect_message!(client.read().await, ReadyForQuery);

    // Cleanup
    client
        .send_simple(Query::new(format!(
            "DELETE FROM sharded WHERE id IN ({}, {})",
            id_shard0, id_shard1
        )))
        .await;
    client.read_until('Z').await.unwrap();
}

#[tokio::test]
async fn test_sharded_insert_returning_from_all_shards() {
    let mut client = TestClient::new_sharded(Parameters::default()).await;

    let id_shard0 = client.random_id_for_shard(0);
    let id_shard1 = client.random_id_for_shard(1);

    // Cleanup first
    client
        .send_simple(Query::new(format!(
            "DELETE FROM sharded WHERE id IN ({}, {})",
            id_shard0, id_shard1
        )))
        .await;
    client.read_until('Z').await.unwrap();

    // Multi-row INSERT with RETURNING - should return rows from ALL shards
    client
        .send_simple(Query::new(format!(
            "INSERT INTO sharded (id, value) VALUES ({}, 'ret1'), ({}, 'ret2') RETURNING id, value",
            id_shard0, id_shard1
        )))
        .await;

    let messages = client.read_until('Z').await.unwrap();

    let data_rows: Vec<_> = messages.iter().filter(|m| m.code() == 'D').collect();

    assert_eq!(
        data_rows.len(),
        2,
        "sharded INSERT RETURNING should return rows from all shards, got {} rows",
        data_rows.len()
    );

    let cc_msg = messages.iter().find(|m| m.code() == 'C').unwrap();
    let cc = CommandComplete::try_from(cc_msg.clone()).unwrap();
    assert_eq!(cc.command(), "INSERT 0 2");

    // Cleanup
    client
        .send_simple(Query::new(format!(
            "DELETE FROM sharded WHERE id IN ({}, {})",
            id_shard0, id_shard1
        )))
        .await;
    client.read_until('Z').await.unwrap();
}

#[tokio::test]
async fn test_sharded_update_sums_row_counts() {
    let mut client = TestClient::new_sharded(Parameters::default()).await;

    // Use the 'sharded' table which is a regular sharded table
    // Insert data that will go to different shards
    let id_shard0 = client.random_id_for_shard(0);
    let id_shard1 = client.random_id_for_shard(1);

    client
        .send_simple(Query::new(format!(
            "INSERT INTO sharded (id, value) VALUES ({}, 'test'), ({}, 'test') ON CONFLICT (id) DO UPDATE SET value = 'test'",
            id_shard0, id_shard1
        )))
        .await;
    client.read_until('Z').await.unwrap();

    // Update all rows - should return SUMMED count from both shards
    client
        .send_simple(Query::new(
            "UPDATE sharded SET value = 'updated' WHERE value = 'test'",
        ))
        .await;

    let cc = expect_message!(client.read().await, CommandComplete);
    // Should be "UPDATE 2" (1 from each shard, summed)
    assert_eq!(
        cc.command(),
        "UPDATE 2",
        "sharded UPDATE should return summed row count from all shards"
    );
    expect_message!(client.read().await, ReadyForQuery);

    // Cleanup
    client
        .send_simple(Query::new(format!(
            "DELETE FROM sharded WHERE id IN ({}, {})",
            id_shard0, id_shard1
        )))
        .await;
    client.read_until('Z').await.unwrap();
}

#[tokio::test]
async fn test_sharded_delete_sums_row_counts() {
    let mut client = TestClient::new_sharded(Parameters::default()).await;

    let id_shard0 = client.random_id_for_shard(0);
    let id_shard1 = client.random_id_for_shard(1);

    client
        .send_simple(Query::new(format!(
            "INSERT INTO sharded (id, value) VALUES ({}, 'del'), ({}, 'del') ON CONFLICT (id) DO UPDATE SET value = 'del'",
            id_shard0, id_shard1
        )))
        .await;
    client.read_until('Z').await.unwrap();

    // Delete all rows - should return SUMMED count
    client
        .send_simple(Query::new("DELETE FROM sharded WHERE value = 'del'"))
        .await;

    let cc = expect_message!(client.read().await, CommandComplete);
    // Should be "DELETE 2" (1 from each shard, summed)
    assert_eq!(
        cc.command(),
        "DELETE 2",
        "sharded DELETE should return summed row count from all shards"
    );
    expect_message!(client.read().await, ReadyForQuery);
}

#[tokio::test]
async fn test_sharded_update_returning_from_all_shards() {
    let mut client = TestClient::new_sharded(Parameters::default()).await;

    let id_shard0 = client.random_id_for_shard(0);
    let id_shard1 = client.random_id_for_shard(1);

    client
        .send_simple(Query::new(format!(
            "INSERT INTO sharded (id, value) VALUES ({}, 'ret'), ({}, 'ret') ON CONFLICT (id) DO UPDATE SET value = 'ret'",
            id_shard0, id_shard1
        )))
        .await;
    client.read_until('Z').await.unwrap();

    // UPDATE with RETURNING - should return rows from ALL shards
    client
        .send_simple(Query::new(
            "UPDATE sharded SET value = 'returned' WHERE value = 'ret' RETURNING id",
        ))
        .await;

    let messages = client.read_until('Z').await.unwrap();

    let data_rows: Vec<_> = messages.iter().filter(|m| m.code() == 'D').collect();

    // Should be 2 rows (1 from each shard)
    assert_eq!(
        data_rows.len(),
        2,
        "sharded UPDATE RETURNING should return rows from all shards, got {} rows",
        data_rows.len()
    );

    let cc_msg = messages.iter().find(|m| m.code() == 'C').unwrap();
    let cc = CommandComplete::try_from(cc_msg.clone()).unwrap();
    assert_eq!(cc.command(), "UPDATE 2");

    // Cleanup
    client
        .send_simple(Query::new(format!(
            "DELETE FROM sharded WHERE id IN ({}, {})",
            id_shard0, id_shard1
        )))
        .await;
    client.read_until('Z').await.unwrap();
}

#[tokio::test]
async fn test_sharded_delete_returning_from_all_shards() {
    let mut client = TestClient::new_sharded(Parameters::default()).await;

    let id_shard0 = client.random_id_for_shard(0);
    let id_shard1 = client.random_id_for_shard(1);

    client
        .send_simple(Query::new(format!(
            "INSERT INTO sharded (id, value) VALUES ({}, 'delret'), ({}, 'delret') ON CONFLICT (id) DO UPDATE SET value = 'delret'",
            id_shard0, id_shard1
        )))
        .await;
    client.read_until('Z').await.unwrap();

    // DELETE with RETURNING - should return rows from ALL shards
    client
        .send_simple(Query::new(
            "DELETE FROM sharded WHERE value = 'delret' RETURNING id",
        ))
        .await;

    let messages = client.read_until('Z').await.unwrap();

    let data_rows: Vec<_> = messages.iter().filter(|m| m.code() == 'D').collect();

    // Should be 2 rows (1 from each shard)
    assert_eq!(
        data_rows.len(),
        2,
        "sharded DELETE RETURNING should return rows from all shards, got {} rows",
        data_rows.len()
    );

    let cc_msg = messages.iter().find(|m| m.code() == 'C').unwrap();
    let cc = CommandComplete::try_from(cc_msg.clone()).unwrap();
    assert_eq!(cc.command(), "DELETE 2");
}

#[tokio::test]
async fn test_sharded_distinct_on_sharding_key_merges_shards() {
    let mut client = TestClient::new_sharded(Parameters::default()).await;

    let id_shard0 = client.random_id_for_shard(0);
    let id_shard1 = client.random_id_for_shard(1);
    let ids = format!("{}, {}", id_shard0, id_shard1);

    // Cleanup first
    client
        .send_simple(Query::new(format!(
            "DELETE FROM sharded WHERE id IN ({ids})"
        )))
        .await;
    client.read_until('Z').await.unwrap();

    client
        .send_simple(Query::new(format!(
            "INSERT INTO sharded (id, value) VALUES ({}, 'cd1'), ({}, 'cd2')",
            id_shard0, id_shard1
        )))
        .await;
    client.read_until('Z').await.unwrap();

    // Each shard counts its own key; no key is on both, so the counts add up.
    client
        .send_simple(Query::new(format!(
            "SELECT count(DISTINCT id), count(DISTINCT id)::int FROM sharded WHERE id IN ({ids})"
        )))
        .await;
    expect_message!(client.read().await, RowDescription);
    let row = expect_message!(client.read().await, DataRow);
    assert_eq!(row.get_int(0, true), Some(2));
    assert_eq!(row.get_int(1, true), Some(2));
    assert_eq!(
        expect_message!(client.read().await, CommandComplete).command(),
        "SELECT 1"
    );
    expect_message!(client.read().await, ReadyForQuery);

    // The sums add up, and each shard's average is weighted by its key count.
    client
        .send_simple(Query::new(format!(
            "SELECT sum(DISTINCT id), avg(DISTINCT id) FROM sharded WHERE id IN ({ids})"
        )))
        .await;
    let description = expect_message!(client.read().await, RowDescription);
    assert_eq!(description.fields.len(), 2, "the count helper is dropped");
    let row = expect_message!(client.read().await, DataRow);
    let value = |column| {
        row.get_text(column)
            .and_then(|text| text.parse::<Decimal>().ok())
    };
    let sum = Decimal::from(id_shard0) + Decimal::from(id_shard1);
    assert_eq!(value(0), Some(sum));
    // Postgres rounds the average of values this large to a whole number.
    let avg = value(1).expect("numeric average");
    assert!((avg - sum / Decimal::TWO).abs() <= Decimal::ONE, "{avg}");
    assert_eq!(
        expect_message!(client.read().await, CommandComplete).command(),
        "SELECT 1"
    );
    expect_message!(client.read().await, ReadyForQuery);

    // Grouped by the key, each group is on one shard, which returns its final
    // result: nothing is merged, so any aggregate works, and so does HAVING.
    client
        .send_simple(Query::new(format!(
            "SELECT id, count(DISTINCT value), string_agg(value, ','), variance(DISTINCT id) \
             FROM sharded WHERE id IN ({ids}) GROUP BY id \
             HAVING count(DISTINCT value) = 1 ORDER BY count(*), id DESC LIMIT 5"
        )))
        .await;
    let description = expect_message!(client.read().await, RowDescription);
    assert_eq!(description.fields.len(), 4, "the sort helper is dropped");
    let mut ids_by_shard = [(id_shard0, "cd1"), (id_shard1, "cd2")];
    ids_by_shard.sort_unstable_by_key(|&(id, _)| std::cmp::Reverse(id));
    for (id, value) in ids_by_shard {
        let row = expect_message!(client.read().await, DataRow);
        assert_eq!(row.get_int(0, true), Some(id));
        assert_eq!(row.get_int(1, true), Some(1));
        assert_eq!(row.get_text(2).as_deref(), Some(value));
        // The variance of a single value is NULL.
        assert!(row.get_raw(3).is_some_and(|variance| variance.is_null));
    }
    assert_eq!(
        expect_message!(client.read().await, CommandComplete).command(),
        "SELECT 2"
    );
    expect_message!(client.read().await, ReadyForQuery);

    // Cleanup
    client
        .send_simple(Query::new(format!(
            "DELETE FROM sharded WHERE id IN ({ids})"
        )))
        .await;
    client.read_until('Z').await.unwrap();
}

#[tokio::test]
async fn test_sharded_distinct_on_invalid_ordinal_returns_postgres_error() {
    let mut client = TestClient::new_sharded(Parameters::default()).await;

    for ordinal in [0, -1] {
        client
            .send_simple(Query::new(format!(
                "SELECT DISTINCT ON ({ordinal}) id FROM sharded"
            )))
            .await;

        let error = expect_message!(client.read().await, ErrorResponse);
        assert_eq!(error.code, "42P10");
        assert_eq!(
            error.message,
            format!("DISTINCT ON position {ordinal} is not in select list")
        );
        // Every shard rejects the query, and nothing but errors precede ReadyForQuery.
        loop {
            let message = client.read().await;
            if message.code() == 'Z' {
                break;
            }
            assert_eq!(message.code(), 'E');
        }
    }
}

#[tokio::test]
async fn test_sharded_distinct_aggregates_over_values_shards_share() {
    let mut client = TestClient::new_sharded(Parameters::default()).await;

    // Both shards have "x", and each has a value of its own.
    let rows = [
        (client.random_id_for_shard(0), "x"),
        (client.random_id_for_shard(0), "yy"),
        (client.random_id_for_shard(1), "x"),
        (client.random_id_for_shard(1), "zzz"),
    ];
    let ids = rows.map(|(id, _)| id.to_string()).join(", ");

    // Cleanup first
    client
        .send_simple(Query::new(format!(
            "DELETE FROM sharded WHERE id IN ({ids})"
        )))
        .await;
    client.read_until('Z').await.unwrap();

    for (id, value) in rows {
        client
            .send_simple(Query::new(format!(
                "INSERT INTO sharded (id, value) VALUES ({id}, '{value}')"
            )))
            .await;
        client.read_until('Z').await.unwrap();
    }

    // Each shard counts 2. PgDog counts the union of their values, not 2 + 2.
    client
        .send_simple(Query::new(format!(
            "SELECT count(DISTINCT value), count(DISTINCT value)::int, count(DISTINCT id), \
             count(DISTINCT value) FILTER (WHERE value <> 'x') \
             FROM sharded WHERE id IN ({ids})"
        )))
        .await;
    let description = expect_message!(client.read().await, RowDescription);
    assert_eq!(description.fields.len(), 4, "the value arrays are dropped");
    let row = expect_message!(client.read().await, DataRow);
    assert_eq!(row.get_int(0, true), Some(3));
    assert_eq!(row.get_int(1, true), Some(3));
    assert_eq!(row.get_int(2, true), Some(4));
    assert_eq!(row.get_int(3, true), Some(2));
    assert_eq!(
        expect_message!(client.read().await, CommandComplete).command(),
        "SELECT 1"
    );
    expect_message!(client.read().await, ReadyForQuery);

    // The lengths are 1 and 2 on one shard, 1 and 3 on the other. The shard
    // sums, 3 and 4, count the shared 1 twice.
    client
        .send_simple(Query::new(format!(
            "SELECT sum(DISTINCT length(value)), avg(DISTINCT length(value)) \
             FROM sharded WHERE id IN ({ids})"
        )))
        .await;
    let description = expect_message!(client.read().await, RowDescription);
    assert_eq!(description.fields.len(), 2, "the value arrays are dropped");
    let row = expect_message!(client.read().await, DataRow);
    assert_eq!(row.get_int(0, true), Some(6));
    assert_eq!(row.get_text(1).as_deref(), Some("2.0000000000000000"));
    assert_eq!(
        expect_message!(client.read().await, CommandComplete).command(),
        "SELECT 1"
    );
    expect_message!(client.read().await, ReadyForQuery);

    // Cleanup
    client
        .send_simple(Query::new(format!(
            "DELETE FROM sharded WHERE id IN ({ids})"
        )))
        .await;
    client.read_until('Z').await.unwrap();
}

/// Regression for https://github.com/pgdogdev/pgdog/issues/2100.
#[tokio::test]
async fn test_sharded_distinct_on_key_missing_from_select_list() {
    let mut client = TestClient::new_sharded(Parameters::default()).await;

    // One row per shard, and a third that repeats the first one's value.
    let mut rows = [
        (client.random_id_for_shard(0), "don_a"),
        (client.random_id_for_shard(1), "don_b"),
        (client.random_id_for_shard(1), "don_a"),
    ];
    rows.sort_unstable();
    let ids = rows.map(|(id, _)| id.to_string()).join(", ");

    // Cleanup first
    client
        .send_simple(Query::new(format!(
            "DELETE FROM sharded WHERE id IN ({ids})"
        )))
        .await;
    client.read_until('Z').await.unwrap();

    for (id, value) in rows {
        client
            .send_simple(Query::new(format!(
                "INSERT INTO sharded (id, value) VALUES ({id}, '{value}')"
            )))
            .await;
        client.read_until('Z').await.unwrap();
    }

    // Every id is its own key, selected or not, under its name or another.
    for select in [
        "value",
        "id AS key, value",
        "sharded.value, sharded.id AS id2",
    ] {
        client
            .send_simple(Query::new(format!(
                "SELECT DISTINCT ON (sharded.id) {select} FROM sharded \
                 WHERE id IN ({ids}) ORDER BY sharded.id"
            )))
            .await;
        let description = expect_message!(client.read().await, RowDescription);
        assert_eq!(
            description.fields.len(),
            select.split(',').count(),
            "the key helper is dropped: {select}"
        );
        for _ in rows {
            expect_message!(client.read().await, DataRow);
        }
        assert_eq!(
            expect_message!(client.read().await, CommandComplete).command(),
            "SELECT 3",
            "{select}"
        );
        expect_message!(client.read().await, ReadyForQuery);
    }

    // Rows that share a key are still reduced to the first one, across shards.
    client
        .send_simple(Query::new(format!(
            "SELECT DISTINCT ON (upper(value)) id FROM sharded WHERE id IN ({ids})"
        )))
        .await;
    let description = expect_message!(client.read().await, RowDescription);
    assert_eq!(description.fields.len(), 1, "the key helper is dropped");
    for _ in 0..2 {
        expect_message!(client.read().await, DataRow);
    }
    assert_eq!(
        expect_message!(client.read().await, CommandComplete).command(),
        "SELECT 2"
    );
    expect_message!(client.read().await, ReadyForQuery);

    // Cleanup
    client
        .send_simple(Query::new(format!(
            "DELETE FROM sharded WHERE id IN ({ids})"
        )))
        .await;
    client.read_until('Z').await.unwrap();
}
