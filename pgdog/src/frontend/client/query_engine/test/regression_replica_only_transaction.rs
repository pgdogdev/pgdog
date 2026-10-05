use super::prelude::*;
use crate::{
    backend::databases::reload_from_existing,
    config::{config, load_test_sharded, set},
    expect_message,
    net::{DataRow, ReadyForQuery},
};
use pgdog_config::{ReadWriteStrategy, Role};

#[tokio::test]
async fn ordinary_transaction_on_replica_only_cluster() {
    load_test_sharded();
    let mut cfg = (*config()).clone();
    cfg.config.general.read_write_strategy = ReadWriteStrategy::Conservative;
    cfg.config
        .databases
        .retain(|database| database.role == Role::Replica);
    set(cfg).unwrap();
    reload_from_existing().unwrap();
    let mut client = TestClient::new(Parameters::default()).await;

    client.send_simple(Query::new("BEGIN")).await;
    client.read_until('Z').await.unwrap();

    client.send_simple(Query::new("SELECT 1")).await;
    let messages = client.read_until('Z').await.unwrap();
    let row = messages
        .iter()
        .find(|message| message.code() == 'D')
        .unwrap();
    assert_eq!(
        expect_message!(row.clone(), DataRow).get_int(0, true),
        Some(1)
    );
    assert_eq!(
        expect_message!(messages.last().unwrap().clone(), ReadyForQuery).status,
        'T'
    );

    client.send_simple(Query::new("ROLLBACK")).await;
    client.read_until('Z').await.unwrap();
}
