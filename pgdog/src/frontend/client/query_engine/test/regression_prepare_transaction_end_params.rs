use super::prelude::*;
use crate::{expect_message, net::DataRow};

async fn prepare_transaction_end_preserves_local_setting(sql: &str) {
    let mut client = TestClient::new_sharded(Parameters::default()).await;
    client.send_simple(Query::new("BEGIN")).await;
    client.read_until('Z').await.unwrap();
    client
        .send_simple(Query::new(
            "SET LOCAL application_name = 'regression_local'",
        ))
        .await;
    client.read_until('Z').await.unwrap();

    client.send(Parse::named("end_transaction", sql)).await;
    client
        .send(Describe::new_statement("end_transaction"))
        .await;
    client.send(Sync).await;
    client.try_process().await.unwrap();
    client.read_until('Z').await.unwrap();

    // The first backend query must still receive the transaction-local setting.
    client
        .send_simple(Query::new(
            "/* pgdog_shard: 0 */ SELECT current_setting('application_name')",
        ))
        .await;
    let messages = client.read_until('Z').await.unwrap();
    let row = messages
        .iter()
        .find(|message| message.code() == 'D')
        .unwrap();
    assert_eq!(
        expect_message!(row.clone(), DataRow).column(0).unwrap(),
        "regression_local".as_bytes(),
        "preparing {sql} must not discard SET LOCAL"
    );

    client.send_simple(Query::new("ROLLBACK")).await;
    client.read_until('Z').await.unwrap();
}

#[tokio::test]
async fn preparing_commit_preserves_transaction_local_settings() {
    prepare_transaction_end_preserves_local_setting("COMMIT").await;
}

#[tokio::test]
async fn preparing_rollback_preserves_transaction_local_settings() {
    prepare_transaction_end_preserves_local_setting("ROLLBACK").await;
}
