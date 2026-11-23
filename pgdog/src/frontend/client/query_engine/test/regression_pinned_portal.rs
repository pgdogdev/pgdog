use super::prelude::*;
use crate::{
    backend::databases::reload_from_existing,
    config::{config, load_test_sharded, set},
    net::Message,
};

fn connected_servers(client: &mut TestClient) -> usize {
    client.engine.backend().connected_servers()
}

fn has_code(messages: &[Message], code: char) -> bool {
    messages.iter().any(|message| message.code() == code)
}

async fn declare_describe_fetch_close(client: &mut TestClient) {
    assert!(
        client.engine.backend().cluster().unwrap().shards().len() > 1,
        "test requires a multi-shard cluster"
    );

    client
        .send(Parse::new_anonymous(
            "DECLARE probe NO SCROLL CURSOR FOR SELECT * FROM sharded",
        ))
        .await;
    client.send(Bind::default()).await;
    client.send(Describe::new_portal("")).await;
    client.send(Execute::new()).await;
    client.send(Sync).await;
    client.try_process().await.unwrap();
    client.read_until('Z').await.unwrap();
    assert_eq!(connected_servers(client), 1);

    client.send(Describe::new_portal("probe")).await;
    client.send(Sync).await;
    client.try_process().await.unwrap();
    let messages = client.read_until('Z').await.unwrap();
    assert!(has_code(&messages, 'T'), "{messages:?}");
    assert_eq!(connected_servers(client), 1);

    client.send_simple(Query::new("FETCH 10 FROM probe")).await;
    client.read_until('Z').await.unwrap();
    assert_eq!(connected_servers(client), 1);

    client.send(Close::portal("probe")).await;
    client.send(Sync).await;
    client.try_process().await.unwrap();
    let messages = client.read_until('Z').await.unwrap();
    assert!(has_code(&messages, '3'), "{messages:?}");
    assert_eq!(connected_servers(client), 1);

    client.send_simple(Query::new("ROLLBACK")).await;
    client.read_until('Z').await.unwrap();
}

#[tokio::test]
async fn portal_messages_follow_set_shard_pin() {
    let mut client = TestClient::new_sharded(Parameters::default()).await;

    client.send_simple(Query::new("BEGIN")).await;
    client.read_until('Z').await.unwrap();
    client.send_simple(Query::new("SET pgdog.shard TO 1")).await;
    client.read_until('Z').await.unwrap();

    declare_describe_fetch_close(&mut client).await;
}

#[tokio::test]
async fn portal_messages_follow_set_sharding_key_pin() {
    load_test_sharded();
    let mut cfg = (*config()).clone();
    cfg.config.sharded_schemas.clear();
    cfg.config
        .sharded_tables
        .retain(|t| t.name.as_deref() == Some("sharded"));
    set(cfg).unwrap();
    reload_from_existing().unwrap();

    let mut client = TestClient::new(Parameters::default()).await;

    client.send_simple(Query::new("BEGIN")).await;
    client.read_until('Z').await.unwrap();
    client
        .send_simple(Query::new("SET pgdog.sharding_key TO '1'"))
        .await;
    client.read_until('Z').await.unwrap();

    declare_describe_fetch_close(&mut client).await;
}
