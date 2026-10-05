use super::prelude::*;
use crate::{
    expect_message,
    net::{NoData, ParameterDescription, ParseComplete, ReadyForQuery},
};

async fn prepare_transaction_end_while_idle(sql: &str) {
    let mut client = TestClient::new_sharded(Parameters::default()).await;

    client.send(Parse::named("end_transaction", sql)).await;
    client
        .send(Describe::new_statement("end_transaction"))
        .await;
    client.send(Sync).await;
    client.try_process().await.unwrap();

    expect_message!(client.read().await, ParseComplete);
    expect_message!(client.read().await, ParameterDescription);
    expect_message!(client.read().await, NoData);
    let messages = client.read_until('Z').await.unwrap();
    assert_eq!(
        expect_message!(messages.last().unwrap().clone(), ReadyForQuery).status,
        'I',
        "preparing {sql} outside a transaction must report idle"
    );
}

#[tokio::test]
async fn preparing_commit_while_idle_reports_idle() {
    prepare_transaction_end_while_idle("COMMIT").await;
}

#[tokio::test]
async fn preparing_rollback_while_idle_reports_idle() {
    prepare_transaction_end_while_idle("ROLLBACK").await;
}
