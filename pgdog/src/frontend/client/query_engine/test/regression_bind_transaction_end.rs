use super::prelude::*;
use crate::{
    backend::pool::Request,
    expect_message,
    net::{BindComplete, CommandComplete, ParseComplete, ReadyForQuery},
};

async fn bind_transaction_end(sql: &str) {
    let mut client = TestClient::new_sharded(Parameters::default()).await;
    let cluster = client.engine.backend.cluster().unwrap().clone();
    let id = client.random_id_for_shard(0);

    client.send_simple(Query::new("BEGIN")).await;
    client.read_until('Z').await.unwrap();

    // Binding creates a portal, but does not execute COMMIT or ROLLBACK.
    client.send(Parse::named("end_transaction", sql)).await;
    client.send(Bind::new_statement("end_transaction")).await;
    client.send(Sync).await;
    client.try_process().await.unwrap();

    expect_message!(client.read().await, ParseComplete);
    expect_message!(client.read().await, BindComplete);
    assert_eq!(
        expect_message!(client.read().await, ReadyForQuery).status,
        'T',
        "binding {sql} without Execute must preserve the transaction"
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

    client.send_simple(Query::new("ROLLBACK")).await;
    client.read_until('Z').await.unwrap();

    let mut server = cluster.primary(0, &Request::default()).await.unwrap();
    let cleanup = server
        .execute(format!("DELETE FROM sharded WHERE id = {id}"))
        .await
        .unwrap();
    let deleted = cleanup
        .iter()
        .find(|message| message.code() == 'C')
        .unwrap();
    assert_eq!(
        expect_message!(deleted.clone(), CommandComplete).command(),
        "DELETE 0",
        "the write after binding {sql} must have been rolled back"
    );
}

#[tokio::test]
#[ignore] // This is somewhat of an insane edge case, but we should still handle it at some point.
async fn binding_commit_without_execute_preserves_transaction() {
    bind_transaction_end("COMMIT").await;
}

#[tokio::test]
#[ignore] // This is somewhat of an insane edge case, but we should still handle it at some point.
async fn binding_rollback_without_execute_preserves_transaction() {
    bind_transaction_end("ROLLBACK").await;
}
