use super::prelude::*;
use crate::{
    backend::databases::reload_from_existing,
    config::{config, load_test, set},
    expect_message,
    net::{CommandComplete, ErrorResponse, Parameters, ParseComplete, ReadyForQuery},
};

/// One server connection, so the statement prepared before BEGIN is still
/// cached on the connection that later runs the aborted transaction.
fn load_single_connection_pool() {
    load_test();

    let mut config = (*config()).clone();
    config.config.general.default_pool_size = 1;
    config.config.general.min_pool_size = 0;
    set(config).unwrap();
    reload_from_existing().unwrap();
}

/// Same sequence as a named `PQsendPrepare` after `SELECT 1/0` aborts an
/// explicit transaction. Both the direct server and the proxy must answer
/// the second Parse with SQLSTATE 25P02.
#[tokio::test]
async fn test_named_reparse_in_aborted_transaction() {
    load_single_connection_pool();
    let mut client = TestClient::new(Parameters::default()).await;
    let name = "scm_aborted_cache";

    client.send(Parse::named(name, "SELECT 1")).await;
    client.send(Sync).await;
    client.try_process().await.unwrap();
    expect_message!(client.read().await, ParseComplete);
    assert_eq!(
        expect_message!(client.read().await, ReadyForQuery).status,
        'I'
    );

    client.send(Query::new("BEGIN")).await;
    client.try_process().await.unwrap();
    expect_message!(client.read().await, CommandComplete);
    assert_eq!(
        expect_message!(client.read().await, ReadyForQuery).status,
        'T'
    );

    client.send(Query::new("SELECT 1/0")).await;
    client.try_process().await.unwrap();
    assert_eq!(
        expect_message!(client.read().await, ErrorResponse).code,
        "22012"
    );
    assert_eq!(
        expect_message!(client.read().await, ReadyForQuery).status,
        'E'
    );

    client.send(Parse::named(name, "SELECT 1")).await;
    client.send(Sync).await;
    client.try_process().await.unwrap();
    assert_eq!(
        expect_message!(client.read().await, ErrorResponse).code,
        "25P02"
    );
    assert_eq!(
        expect_message!(client.read().await, ReadyForQuery).status,
        'E'
    );

    client.send(Query::new("ROLLBACK")).await;
    client.try_process().await.unwrap();
    expect_message!(client.read().await, CommandComplete);
    assert_eq!(
        expect_message!(client.read().await, ReadyForQuery).status,
        'I'
    );
}
