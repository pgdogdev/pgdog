use super::prelude::*;
use crate::{expect_message, net::ReadyForQuery};

#[tokio::test]
async fn standalone_sync_preserves_shard_pinned_transaction() {
    let mut client = TestClient::new_sharded(Parameters::default()).await;
    client.send_simple(Query::new("SET pgdog.shard = 0")).await;
    client.read_until('Z').await.unwrap();
    client.send_simple(Query::new("BEGIN")).await;
    client.read_until('Z').await.unwrap();

    client.send(Parse::named("pinned_select", "SELECT 1")).await;
    client.send(Bind::new_statement("pinned_select")).await;
    client.send(Execute::new()).await;
    client.send(Flush).await;
    client.try_process().await.unwrap();
    client.read_until('C').await.unwrap();

    // Sync has no SQL route; it must finish the exchange on the existing shard.
    client.send(Sync).await;
    client.try_process().await.unwrap();
    assert_eq!(
        expect_message!(client.read().await, ReadyForQuery).status,
        'T',
        "Sync must not be rejected as a shard switch"
    );

    client.send_simple(Query::new("ROLLBACK")).await;
    client.read_until('Z').await.unwrap();
}
