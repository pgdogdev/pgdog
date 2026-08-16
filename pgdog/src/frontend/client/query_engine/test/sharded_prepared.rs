use crate::net::{CommandComplete, DataRow, Message};

use super::prelude::*;
use super::*;

async fn query(client: &mut TestClient, sql: impl ToString) -> Vec<Message> {
    client.send_simple(Query::new(sql)).await;
    client.read_until('Z').await.unwrap()
}

fn assert_command(messages: &[Message], expected: &str) {
    let message = messages
        .iter()
        .find(|message| message.code() == 'C')
        .unwrap();
    let command = CommandComplete::try_from(message.clone()).unwrap();
    assert_eq!(command.command(), expected);
}

async fn prepared_crud(client: &mut TestClient, table: &str, id: i64, reads: usize) {
    query(client, format!("DELETE FROM {table} WHERE id = {id}")).await;

    query(
        client,
        format!("PREPARE insert_stmt AS INSERT INTO {table} (id, value) VALUES ($1, $2)"),
    )
    .await;
    let messages = query(client, format!("EXECUTE insert_stmt({id}, 'inserted')")).await;
    assert_command(&messages, "INSERT 0 1");

    query(
        client,
        format!("PREPARE select_stmt AS SELECT value FROM {table} WHERE id = $1"),
    )
    .await;
    for _ in 0..reads {
        let messages = query(client, format!("EXECUTE select_stmt({id})")).await;
        let row = messages
            .iter()
            .find(|message| message.code() == 'D')
            .unwrap();
        let row = DataRow::try_from(row.clone()).unwrap();
        assert_eq!(row.get_text(0).as_deref(), Some("inserted"));
    }

    query(
        client,
        format!("PREPARE update_stmt AS UPDATE {table} SET value = $2 WHERE id = $1"),
    )
    .await;
    let messages = query(client, format!("EXECUTE update_stmt({id}, 'updated')")).await;
    assert_command(&messages, "UPDATE 1");

    for _ in 0..reads {
        let messages = query(client, format!("EXECUTE select_stmt({id})")).await;
        let row = messages
            .iter()
            .find(|message| message.code() == 'D')
            .unwrap();
        let row = DataRow::try_from(row.clone()).unwrap();
        assert_eq!(row.get_text(0).as_deref(), Some("updated"));
    }

    query(
        client,
        format!("PREPARE delete_stmt AS DELETE FROM {table} WHERE id = $1"),
    )
    .await;
    let messages = query(client, format!("EXECUTE delete_stmt({id})")).await;
    assert_command(&messages, "DELETE 1");

    for _ in 0..reads {
        let messages = query(client, format!("EXECUTE select_stmt({id})")).await;
        assert!(!messages.iter().any(|message| message.code() == 'D'));
        assert_command(&messages, "SELECT 0");
    }
}

#[tokio::test]
async fn test_sharded_prepared_crud() {
    crate::logger();

    let mut client = TestClient::new_sharded(Parameters::default())
        .await
        .with_full_prepared_statements();
    let id = client.random_id_for_shard(1);

    prepared_crud(&mut client, "sharded", id, 1).await;
}

#[tokio::test]
async fn test_omnisharded_prepared_crud() {
    crate::logger();

    let mut client = TestClient::new_sharded(Parameters::default())
        .await
        .with_full_prepared_statements();
    let id = client.random_id_for_shard(1);

    prepared_crud(&mut client, "sharded_omni", id, 2).await;
}

#[tokio::test]
async fn test_cross_shard_prepared() {
    let mut client = TestClient::new_sharded(Parameters::default())
        .await
        .with_full_prepared_statements();

    // Clean up after bad tests.
    client.send_simple(Query::new("DELETE FROM sharded")).await;
    client.read_until('Z').await.unwrap();

    client
        .send_simple(Query::new(
            "PREPARE __stmt_1 AS INSERT INTO sharded (id, value) VALUES ($1, $2)",
        ))
        .await;
    client.read_until('Z').await.unwrap();
    client
        .send_simple(Query::new(
            "PREPARE __stmt_2 AS SELECT id, value FROM sharded ORDER BY id DESC",
        ))
        .await;
    client.read_until('Z').await.unwrap();

    for shard in 0..20 {
        let id = client.random_id_for_shard(shard % 2);
        client
            .send_simple(Query::new(format!("EXECUTE __stmt_1({}, 'value')", id)))
            .await;
        client.read_until('Z').await.unwrap();
    }

    client.send_simple(Query::new("EXECUTE __stmt_2")).await;
    let msgs = client.read_until('Z').await.unwrap();

    client
        .send_simple(Query::new("PREPARE __stmt_3 AS DELETE FROM sharded"))
        .await;
    client.read_until('Z').await.unwrap();

    client.send_simple(Query::new("EXECUTE __stmt_3")).await;
    client.read_until('Z').await.unwrap();

    assert_eq!(msgs.iter().filter(|m| m.code() == 'D').count(), 20);
    let ids = msgs
        .iter()
        .filter(|m| m.code() == 'D')
        .map(|m| {
            DataRow::try_from(m.clone())
                .unwrap()
                .get_int(0, true)
                .unwrap()
        })
        .collect::<Vec<_>>();
    let mut sorted = ids.clone();
    sorted.sort();

    assert_eq!(sorted.into_iter().rev().collect::<Vec<_>>(), ids);

    client.send_simple(Query::new("EXECUTE __stmt_2")).await;
    let msgs = client.read_until('Z').await.unwrap();

    assert_eq!(msgs.iter().filter(|m| m.code() == 'D').count(), 0);
}
