//! `SET ROLE` must not leak from one client to the next in transaction mode.
//!
//! <https://github.com/pgdogdev/pgdog/issues/1341>

use crate::{
    backend::databases::reload_from_existing,
    config::{config, load_test, set},
    expect_message,
    net::{CommandComplete, DataRow, ReadyForQuery, RowDescription},
};

use super::prelude::*;

/// Run a statement that returns no rows.
///
/// Tolerates `ParameterStatus`: `session_authorization` is a reported (GUC_REPORT)
/// parameter, so `SET SESSION AUTHORIZATION` emits an extra 'S' message that
/// `SET ROLE` does not.
async fn run_simple(client: &mut TestClient, query: &str) -> ReadyForQuery {
    client.send_simple(Query::new(query)).await;

    let mut command_complete = false;
    loop {
        let message = client.read().await;
        match message.code() {
            'S' => continue,
            'C' => {
                expect_message!(message, CommandComplete);
                command_complete = true;
            }
            _ => {
                assert!(
                    command_complete,
                    "expected CommandComplete before ReadyForQuery for {query:?}"
                );
                return expect_message!(message, ReadyForQuery);
            }
        }
    }
}

/// Read a single-row, single-column text result through the proxy.
async fn fetch_text(client: &mut TestClient, query: &str) -> String {
    client.send_simple(Query::new(query)).await;
    expect_message!(client.read().await, RowDescription);
    let row = expect_message!(client.read().await, DataRow);
    let value = row.get_text(0).expect("one text column");
    client.read_until('Z').await.unwrap();
    value
}

fn load_single_connection_test_pool() {
    load_test();

    let mut config = (*config()).clone();
    config.config.general.default_pool_size = 1;
    config.config.general.min_pool_size = 0;
    set(config).unwrap();
    reload_from_existing().unwrap();
}

/// The reported bug. Pinning the backend marks it dirty, so check-in runs the
/// `DIRTY` cleanup queries — and `RESET ALL` does not clear `role`. The role
/// survives on the backend while check-in clears `client_params`, so the
/// check-out path no longer knows to reset it either.
#[tokio::test]
async fn test_set_role_does_not_leak_to_next_client() {
    load_single_connection_test_pool();

    let pinned_pid = {
        // `leak_pool`: dropping a TestClient otherwise shuts the pools down, and the
        // second client would get a brand new backend, making the assertion vacuous.
        let mut client = TestClient::new(Parameters::default()).await.leak_pool();

        assert_eq!(
            run_simple(&mut client, "SET pgdog.pin TO true")
                .await
                .status,
            'I'
        );

        // Attaches and locks the backend. `SET ROLE` has to come after this: with no
        // backend attached the SET is answered locally and only materialises at
        // check-out, so it would never reach this connection.
        let pid = client.backend_pid().await;
        assert!(client.backend_locked());

        assert_eq!(run_simple(&mut client, "SET ROLE pgdog1").await.status, 'I');
        assert_eq!(
            fetch_text(&mut client, "SELECT current_user").await,
            "pgdog1"
        );

        pid
    };

    let mut next = TestClient::new(Parameters::default()).await;
    assert_eq!(
        next.backend_pid().await,
        pinned_pid,
        "single connection test pool should reuse the same backend"
    );
    assert_eq!(
        fetch_text(&mut next, "SELECT current_user").await,
        "pgdog",
        "SET ROLE leaked to the next client"
    );
}

/// Same shape for `SET SESSION AUTHORIZATION`, which also leaks `session_user`.
/// `session_authorization` is in `UNTRACKED_PARAMS`, so unlike `role` it is never
/// synced or reset by the check-out path at all.
///
/// Requires the connecting user to be a superuser; `integration/setup.sh` creates
/// `pgdog` and `pgdog1` as `LOGIN SUPERUSER`.
#[tokio::test]
async fn test_set_session_authorization_does_not_leak_to_next_client() {
    load_single_connection_test_pool();

    let pinned_pid = {
        let mut client = TestClient::new(Parameters::default()).await.leak_pool();

        assert_eq!(
            run_simple(&mut client, "SET pgdog.pin TO true")
                .await
                .status,
            'I'
        );

        let pid = client.backend_pid().await;
        assert!(client.backend_locked());

        assert_eq!(
            run_simple(&mut client, "SET SESSION AUTHORIZATION pgdog1")
                .await
                .status,
            'I'
        );
        assert_eq!(
            fetch_text(&mut client, "SELECT session_user").await,
            "pgdog1"
        );

        pid
    };

    let mut next = TestClient::new(Parameters::default()).await;
    assert_eq!(
        next.backend_pid().await,
        pinned_pid,
        "single connection test pool should reuse the same backend"
    );
    assert_eq!(
        fetch_text(&mut next, "SELECT session_user").await,
        "pgdog",
        "SET SESSION AUTHORIZATION leaked to the next client"
    );
}

/// Without a pin the connection is never dirty, so no cleanup runs, `client_params`
/// still records `role`, and the check-out path resets it. This passes before the
/// fix as well as after it — it is here to document that the pin is what breaks the
/// invariant, and to catch a regression in the check-out reset path.
#[tokio::test]
async fn test_set_role_without_pin_does_not_leak() {
    load_single_connection_test_pool();

    let pid = {
        let mut client = TestClient::new(Parameters::default()).await.leak_pool();

        let pid = client.backend_pid().await;
        assert_eq!(run_simple(&mut client, "SET ROLE pgdog1").await.status, 'I');
        assert_eq!(
            fetch_text(&mut client, "SELECT current_user").await,
            "pgdog1"
        );

        pid
    };

    let mut next = TestClient::new(Parameters::default()).await;
    assert_eq!(
        next.backend_pid().await,
        pid,
        "single connection test pool should reuse the same backend"
    );
    assert_eq!(
        fetch_text(&mut next, "SELECT current_user").await,
        "pgdog",
        "SET ROLE leaked to the next client without a pin"
    );
}
