use super::prelude::*;
use crate::{
    backend::pool::Request,
    expect_message,
    net::{CommandComplete, DataRow, NoData, ParameterDescription, ParseComplete, ReadyForQuery},
};

async fn prepare_transaction_end(sql: &str) {
    let mut client = TestClient::new_sharded(Parameters::default()).await;
    let cluster = client.engine.backend.cluster().unwrap().clone();
    let id = client.random_id_for_shard(0);
    client.send_simple(Query::new("BEGIN")).await;
    client.read_until('Z').await.unwrap();
    assert!(
        !client.backend_connected(),
        "BEGIN should still be buffered"
    );

    // Prepare and describe the command, but do not bind or execute it.
    client.send(Parse::named("end_transaction", sql)).await;
    client
        .send(Describe::new_statement("end_transaction"))
        .await;
    client.send(Sync).await;
    client.try_process().await.unwrap();

    expect_message!(client.read().await, ParseComplete);
    expect_message!(client.read().await, ParameterDescription);
    expect_message!(client.read().await, NoData);
    assert_eq!(
        expect_message!(client.read().await, ReadyForQuery).status,
        'T',
        "preparing {sql} must not end the transaction"
    );
    assert!(
        client.client.transaction.is_some(),
        "preparing {sql} must preserve client transaction state"
    );

    client
        .send_simple(Query::new(format!(
            "INSERT INTO sharded (id) VALUES ({id})"
        )))
        .await;
    assert_eq!(
        expect_message!(client.read().await, CommandComplete).command(),
        "INSERT 0 1"
    );
    expect_message!(client.read().await, ReadyForQuery);

    // The write must be visible inside the transaction before rollback.
    client
        .send_simple(Query::new(format!(
            "SELECT id FROM sharded WHERE id = {id}"
        )))
        .await;
    let messages = client.read_until('Z').await.unwrap();
    let row = messages
        .iter()
        .find(|message| message.code() == 'D')
        .unwrap();
    assert_eq!(
        expect_message!(row.clone(), DataRow).get_int(0, true),
        Some(id)
    );

    client.send_simple(Query::new("ROLLBACK")).await;
    client.read_until('Z').await.unwrap();

    // Verify the effect on PostgreSQL directly, independently of client state.
    let mut server = cluster.primary(0, &Request::default()).await.unwrap();
    let rows: Vec<i64> = server
        .fetch_all(format!("SELECT id FROM sharded WHERE id = {id}"))
        .await
        .unwrap();
    assert!(
        rows.is_empty(),
        "the write after preparing {sql} must be rolled back"
    );
}

#[tokio::test]
async fn preparing_commit_preserves_buffered_transaction() {
    prepare_transaction_end("COMMIT").await;
}

#[tokio::test]
async fn preparing_rollback_preserves_buffered_transaction() {
    prepare_transaction_end("ROLLBACK").await;
}
