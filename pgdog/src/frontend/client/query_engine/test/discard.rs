use crate::{
    backend::databases::reload_from_existing,
    config::{config, load_test, set},
    expect_message,
    net::{
        CommandComplete, DataRow, ErrorResponse, Parameters, ParseComplete, ReadyForQuery,
        RowDescription, parameter::ParameterValue,
    },
};

use super::prelude::*;

/// Run a query that returns command tag.
async fn run_simple(client: &mut TestClient, query: &str) -> ReadyForQuery {
    client.send_simple(Query::new(query)).await;
    expect_message!(client.read().await, CommandComplete);
    expect_message!(client.read().await, ReadyForQuery)
}

/// Ask the server what a parameter is actually set to.
async fn show(client: &mut TestClient, name: &str) -> String {
    client
        .send_simple(Query::new(format!("SHOW {}", name)))
        .await;
    expect_message!(client.read().await, RowDescription);
    let row = expect_message!(client.read().await, DataRow);
    let value = row.get_text(0).expect("SHOW returns a value");
    client.read_until('Z').await.unwrap();
    value
}

/// A pool with a single connection, so a client is guaranteed to get the same
/// server back after it's released.
fn load_single_connection_test_pool() {
    load_test();

    let mut config = (*config()).clone();
    config.config.general.default_pool_size = 1;
    config.config.general.min_pool_size = 0;
    set(config).unwrap();
    reload_from_existing().unwrap();
}

fn startup_params(params: &[(&str, &str)]) -> Parameters {
    let mut startup = Parameters::default();
    for (name, value) in params {
        startup.insert(*name, *value);
    }
    startup
}

#[tokio::test]
async fn test_discard_clears_prepared_statement_cache() {
    let mut client = TestClient::new_replicas(Parameters::default())
        .await
        .with_full_prepared_statements();

    client.send(Parse::named("test_stmt", "SELECT $1")).await;
    client.send(Sync).await;
    client.try_process().await.unwrap();

    expect_message!(client.read().await, ParseComplete);
    expect_message!(client.read().await, ReadyForQuery);

    assert_eq!(client.client().prepared_statements.num_statements(), 1);
    let global = client.client().prepared_statements.global.clone();
    assert_eq!(global.read().statements().iter().next().unwrap().1.used, 1);

    client.send_simple(Query::new("DISCARD ALL")).await;

    expect_message!(client.read().await, CommandComplete);
    expect_message!(client.read().await, ReadyForQuery);

    assert_eq!(client.client().prepared_statements.num_statements(), 0);
    assert_eq!(global.read().statements().iter().next().unwrap().1.used, 0);
}

#[tokio::test]
async fn test_non_all_discard_keeps_prepared_statement_cache() {
    for query in ["DISCARD PLANS", "DISCARD SEQUENCES", "DISCARD TEMP"] {
        let mut client = TestClient::new_replicas(Parameters::default())
            .await
            .with_full_prepared_statements();

        client.send(Parse::named("test_stmt", "SELECT $1")).await;
        client.send(Sync).await;
        client.try_process().await.unwrap();

        expect_message!(client.read().await, ParseComplete);
        expect_message!(client.read().await, ReadyForQuery);

        let global = client.client().prepared_statements.global.clone();
        assert_eq!(client.client().prepared_statements.num_statements(), 1);
        assert_eq!(global.read().statements().iter().next().unwrap().1.used, 1);

        client.send_simple(Query::new(query)).await;

        expect_message!(client.read().await, CommandComplete);
        expect_message!(client.read().await, ReadyForQuery);

        assert_eq!(
            client.client().prepared_statements.num_statements(),
            1,
            "{query} should not clear prepared statements",
        );
        assert_eq!(global.read().statements().iter().next().unwrap().1.used, 1);
    }
}

#[tokio::test]
async fn test_discard_all_restores_startup_parameters() {
    let mut client =
        TestClient::new_sharded(startup_params(&[("application_name", "from_startup")])).await;

    run_simple(&mut client, "SET application_name TO 'changed'").await;
    assert_eq!(
        client.client().params.get("application_name"),
        Some(&ParameterValue::String("changed".into())),
    );

    client.send_simple(Query::new("DISCARD ALL")).await;
    assert_eq!(
        expect_message!(client.read().await, CommandComplete).command(),
        "DISCARD"
    );
    assert_eq!(
        expect_message!(client.read().await, ReadyForQuery).status,
        'I'
    );

    assert_eq!(
        client.client().params.get("application_name"),
        Some(&ParameterValue::String("from_startup".into())),
        "DISCARD ALL should restore the startup value, not the one set at runtime",
    );
}

#[tokio::test]
async fn test_discard_all_drops_parameters_missing_from_startup() {
    let mut client = TestClient::new_sharded(Parameters::default()).await;

    run_simple(&mut client, "SET statement_timeout TO 5000").await;
    run_simple(&mut client, "SET search_path TO acustomer").await;

    client.send_simple(Query::new("DISCARD ALL")).await;
    client.read_until('Z').await.unwrap();

    assert_eq!(client.client().params.get("statement_timeout"), None);
    assert_eq!(client.client().params.get("search_path"), None);
}

#[tokio::test]
async fn test_non_all_discard_keeps_parameters() {
    for query in ["DISCARD PLANS", "DISCARD SEQUENCES", "DISCARD TEMP"] {
        let mut client =
            TestClient::new_sharded(startup_params(&[("application_name", "from_startup")])).await;

        run_simple(&mut client, "SET application_name TO 'changed'").await;

        client.send_simple(Query::new(query)).await;
        client.read_until('Z').await.unwrap();

        assert_eq!(
            client.client().params.get("application_name"),
            Some(&ParameterValue::String("changed".into())),
            "{query} should not reset parameters",
        );
    }
}

#[tokio::test]
async fn test_discard_all_fails_inside_transaction() {
    let mut client = TestClient::new_sharded(Parameters::default()).await;

    run_simple(&mut client, "BEGIN").await;
    client.send_simple(Query::new("DISCARD ALL")).await;

    let error = expect_message!(client.read().await, ErrorResponse);
    assert_eq!(error.code, "25001");
    assert_eq!(
        error.message,
        "DISCARD ALL cannot run inside a transaction block"
    );
    assert_eq!(
        expect_message!(client.read().await, ReadyForQuery).status,
        'E'
    );
    assert!(
        client
            .client()
            .transaction
            .is_some_and(|state| state.error()),
        "the transaction should be aborted"
    );
}

#[tokio::test]
async fn test_discard_all_resets_parameters_on_a_pinned_server() {
    load_single_connection_test_pool();
    let mut client = TestClient::new(Parameters::default()).await;

    run_simple(&mut client, "SET pgdog.pin TO true").await;
    run_simple(&mut client, "SET statement_timeout TO 5000").await;

    assert_eq!(show(&mut client, "statement_timeout").await, "5s");
    let pinned_backend_pid = client.backend_pid().await;

    client.send_simple(Query::new("DISCARD ALL")).await;
    client.read_until('Z').await.unwrap();

    assert!(
        !client.backend_locked(),
        "DISCARD ALL should release the pin taken with pgdog.pin"
    );
    assert_eq!(
        show(&mut client, "statement_timeout").await,
        "0",
        "DISCARD ALL should reset the parameter on the server too"
    );
    assert_eq!(
        client.backend_pid().await,
        pinned_backend_pid,
        "single connection test pool should reuse the same backend"
    );
}

#[tokio::test]
async fn test_discard_all_reapplies_startup_parameters_to_the_server() {
    load_single_connection_test_pool();
    let mut client = TestClient::new(startup_params(&[(
        "application_name",
        "test_discard_startup",
    )]))
    .await;

    run_simple(&mut client, "SET pgdog.pin TO true").await;
    run_simple(&mut client, "SET application_name TO 'changed'").await;
    assert_eq!(show(&mut client, "application_name").await, "changed");

    client.send_simple(Query::new("DISCARD ALL")).await;
    client.read_until('Z').await.unwrap();

    assert_eq!(
        show(&mut client, "application_name").await,
        "test_discard_startup",
        "the startup value should be re-applied to the server after the reset"
    );
}
