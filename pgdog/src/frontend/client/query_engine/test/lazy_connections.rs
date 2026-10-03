use super::prelude::*;
use crate::{
    backend::pool::Guard,
    net::{ErrorResponse, Message, ReadyForQuery},
};

async fn query(client: &mut TestClient, sql: impl Into<String>) -> Vec<Message> {
    client.send_simple(Query::new(sql.into())).await;
    let mut messages = Vec::new();
    loop {
        let message = client.read().await;
        let done = message.code() == 'Z';
        messages.push(message);
        if done {
            return messages;
        }
    }
}

#[tokio::test]
async fn lazy_single_shard_is_lazy() {
    let mut client = TestClient::new_sharded(Parameters::default()).await;
    let id = client.random_id_for_shard(0);
    query(&mut client, "BEGIN").await;
    assert_eq!(client.engine.backend().connected_servers(), 0);
    let messages = query(
        &mut client,
        format!("INSERT INTO sharded (id) VALUES ({id})"),
    )
    .await;
    assert!(messages.iter().all(|m| m.code() != 'E'));
    assert_eq!(client.engine.backend().connected_servers(), 1);
    query(&mut client, "COMMIT").await;
    assert_eq!(client.engine.backend().connected_servers(), 0);
    query(&mut client, format!("DELETE FROM sharded WHERE id = {id}")).await;
}

#[tokio::test]
async fn lazy_same_shard_batch_insert() {
    let mut client = TestClient::new_sharded_3(Parameters::default()).await;
    let id0 = client.random_id_for_shard(0);
    let id1 = client.random_id_for_shard(0);
    query(&mut client, "BEGIN").await;
    let result = query(
        &mut client,
        format!("INSERT INTO sharded (id) VALUES ({id0}), ({id1})"),
    )
    .await;
    let connected = client.engine.backend().connected_servers();
    query(&mut client, "ROLLBACK").await;
    assert!(result.iter().all(|m| m.code() != 'E'));
    assert_eq!(
        connected, 1,
        "single-shard batch must only connect to its target"
    );
}

#[tokio::test]
async fn lazy_failed_transaction_cannot_add_shard() {
    let mut client = TestClient::new_sharded(Parameters::default()).await;
    let id0 = client.random_id_for_shard(0);
    let id1 = client.random_id_for_shard(1);
    query(&mut client, "BEGIN").await;
    let failed = query(
        &mut client,
        format!("SELECT 1 / 0 FROM sharded WHERE id = {id0}"),
    )
    .await;
    assert!(failed.iter().any(|m| m.code() == 'E'));
    assert_eq!(
        ReadyForQuery::try_from(failed.last().unwrap().clone())
            .unwrap()
            .status,
        'E'
    );
    assert_eq!(client.engine.backend().connected_servers(), 1);
    let next = query(
        &mut client,
        format!("INSERT INTO sharded (id) VALUES ({id1})"),
    )
    .await;
    query(&mut client, "ROLLBACK").await;
    let errors: Vec<_> = next
        .iter()
        .filter(|m| m.code() == 'E')
        .map(|m| ErrorResponse::try_from(m.clone()).unwrap())
        .collect();
    assert!(
        errors.iter().any(|e| e.code == "25P02"),
        "expected aborted transaction rejection, got {next:?}"
    );
}

#[tokio::test]
async fn lazy_sync_does_not_add_shards() {
    let mut client = TestClient::new_sharded_3(Parameters::default()).await;
    let id = client.random_id_for_shard(0);
    query(&mut client, "BEGIN").await;
    client
        .send(Parse::named(
            "review",
            format!("SELECT * FROM sharded WHERE id = {id}"),
        ))
        .await;
    client.send(Bind::new_statement("review")).await;
    client.send(Execute::new()).await;
    client.send(Flush).await;
    client.try_process().await.unwrap();
    client.read_until('C').await.unwrap();
    assert_eq!(client.engine.backend().connected_servers(), 1);
    client.send(Sync).await;
    client.try_process().await.unwrap();
    client.read_until('Z').await.unwrap();
    let connected = client.engine.backend().connected_servers();
    query(&mut client, "ROLLBACK").await;
    assert_eq!(connected, 1, "Sync should not enlist extra shards");
}

#[tokio::test]
async fn lazy_two_pc_preserves_pinned_connection() {
    let mut client = TestClient::new_sharded_two_pc(Parameters::default()).await;
    query(&mut client, "SET pgdog.pin TO true").await;
    query(&mut client, "BEGIN").await;
    query(&mut client, "SELECT * FROM sharded WHERE false").await;
    assert!(client.backend_locked());
    assert_eq!(client.engine.backend().connected_servers(), 2);
    let result = query(&mut client, "COMMIT").await;
    assert!(result.iter().all(|m| m.code() != 'E'));
    assert!(
        client.backend_connected(),
        "2pc COMMIT must preserve pinned backends"
    );
}

/// Enable client recovery and hold shard 1's only primary connection to force checkout failures.
async fn recovery_setup() -> (SpawnedClient, Guard) {
    use crate::{
        backend::{
            databases::{databases, reload_from_existing},
            pool::Request,
        },
        config::{config, load_test_sharded, set},
    };
    load_test_sharded();
    let mut cfg = (*config()).clone();
    cfg.config.general.default_pool_size = 1;
    cfg.config.general.checkout_timeout = 1000;
    cfg.config.general.client_connection_recovery =
        pgdog_config::pooling::ConnectionRecovery::Recover;
    set(cfg).unwrap();
    reload_from_existing().unwrap();
    let client = SpawnedClient::new(Parameters::default()).await;
    let cluster = databases().cluster(("pgdog", "pgdog")).unwrap();
    let held = cluster.primary(1, &Request::default()).await.unwrap();

    (client, held)
}

#[tokio::test]
async fn lazy_checkout_failure_disconnects_transaction() {
    use std::time::Duration;

    use tokio::{io::AsyncReadExt, time::timeout};

    let (mut client, held) = recovery_setup().await;

    client.send(Query::new("BEGIN")).await;
    client.read_until('Z').await;
    client
        .send(Query::new("/* pgdog_shard: 0 */ SELECT 1"))
        .await;
    let started = client.read_until('Z').await;
    assert_eq!(
        ReadyForQuery::try_from(started.last().unwrap().clone())
            .unwrap()
            .status,
        'T'
    );

    // Shard 1's only connection is held, so adding it to the transaction must fail.
    client
        .send(Query::new("/* pgdog_shard: 1 */ SELECT 1"))
        .await;
    let failed = timeout(Duration::from_secs(5), client.read_until('E'))
        .await
        .expect("checkout failure must reach the client");
    let error = ErrorResponse::try_from(failed.last().unwrap().clone()).unwrap();
    assert_eq!(error.severity, "FATAL");
    assert!(error.message.contains("checkout timeout"), "{error:?}");

    // Even with recovery enabled, an active transaction cannot continue on a new backend.
    let mut buf = [0u8; 1];
    let bytes = timeout(Duration::from_secs(1), client.conn.read(&mut buf))
        .await
        .expect("client must disconnect after checkout failure")
        .expect("read EOF");
    assert_eq!(bytes, 0, "expected EOF without a ReadyForQuery response");

    client.join().await;
    drop(held);
}

#[tokio::test]
async fn lazy_checkout_failure_outside_transaction_keeps_client_usable() {
    use std::time::Duration;

    use tokio::time::timeout;

    use crate::{expect_message, net::CommandComplete};

    let (mut client, held) = recovery_setup().await;
    // A write targets the exhausted primary pool without changing any rows.
    let sql = "/* pgdog_shard: 1 */ UPDATE sharded SET value = value WHERE false";
    client.send(Query::new(sql)).await;
    let failed = timeout(Duration::from_secs(5), client.read_until('E'))
        .await
        .expect("checkout failure must reach the client");
    let error = ErrorResponse::try_from(failed.last().unwrap().clone()).unwrap();
    assert_eq!(error.severity, "ERROR");
    assert!(error.message.contains("checkout timeout"), "{error:?}");
    assert_eq!(
        expect_message!(client.read().await, ReadyForQuery).status,
        'I'
    );

    drop(held);

    // Retry on the same socket after the pool has a connection available.
    client.send(Query::new(sql)).await;
    let succeeded = timeout(Duration::from_secs(5), client.read_until('Z'))
        .await
        .expect("client must remain usable after checkout failure");
    let command = succeeded
        .iter()
        .find(|message| message.code() == 'C')
        .unwrap();
    assert_eq!(
        CommandComplete::try_from(command.clone())
            .unwrap()
            .command(),
        "UPDATE 0"
    );
    assert_eq!(
        ReadyForQuery::try_from(succeeded.last().unwrap().clone())
            .unwrap()
            .status,
        'I'
    );

    client.send(Terminate).await;
    client.join().await;
}
