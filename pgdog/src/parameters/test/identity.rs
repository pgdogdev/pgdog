//! `role` and `session_authorization` tracking.

use crate::{net::Query, parameters::*};

fn queries(sync: ParametersSync<'_>) -> Vec<String> {
    sync.into_iter()
        .map(|query| query.query().to_owned())
        .collect()
}

#[test]
fn test_role_sync_and_reset() {
    let server = ParameterStore::default();
    let mut client = ParameterStore::default();
    client.set(ROLE, "reporting");

    assert_eq!(
        queries(server.sync(&client)),
        [r#"SET "role" TO "reporting""#]
    );

    let mut server = ParameterStore::default();
    server.sync_complete(server.clone().sync(&client));
    let peer = ParameterStore::default();
    assert_eq!(queries(server.sync(&peer)), [r#"RESET "role""#]);
}

#[test]
fn test_session_authorization_is_applied_before_role() {
    let server = ParameterStore::default();
    let mut client = ParameterStore::default();
    client.set(SESSION_AUTHORIZATION, "delegated");
    client.set(ROLE, "reporting");
    client.set("application_name", "app");

    assert_eq!(
        queries(server.sync(&client)),
        [
            r#"SET "session_authorization" TO "delegated""#,
            r#"SET "role" TO "reporting""#,
            r#"SET "application_name" TO "app""#,
        ]
    );
}

#[test]
fn test_session_authorization_change_reapplies_unchanged_role() {
    let mut server = ParameterStore::default();
    server.set(SESSION_AUTHORIZATION, "first");
    server.set(ROLE, "reporting");

    let mut client = ParameterStore::default();
    client.set(SESSION_AUTHORIZATION, "second");
    client.set(ROLE, "reporting");

    assert_eq!(
        queries(server.sync(&client)),
        [
            r#"SET "session_authorization" TO "second""#,
            r#"SET "role" TO "reporting""#,
        ]
    );

    // Resetting the session authorization clears the role too.
    let mut client = ParameterStore::default();
    client.set(ROLE, "reporting");
    assert_eq!(
        queries(server.sync(&client)),
        [
            r#"RESET "session_authorization""#,
            r#"SET "role" TO "reporting""#,
        ]
    );
}

#[test]
fn test_transaction_sync_orders_identity() {
    let mut client = ParameterStore::default();
    client.begin();
    client.set(ROLE, "reporting");
    client.set(SESSION_AUTHORIZATION, "delegated");
    client.set("application_name", "app");

    assert_eq!(
        client.transaction_sync(),
        vec![
            Query::new(r#"SET "session_authorization" TO "delegated""#),
            Query::new(r#"SET "role" TO "reporting""#),
            Query::new(r#"SET "application_name" TO "app""#),
        ]
    );
}

#[test]
fn test_reset_all_keeps_identity() {
    let mut params = ParameterStore::default();
    params.set(ROLE, "reporting");
    params.set(SESSION_AUTHORIZATION, "delegated");
    params.set("work_mem", "1MB");

    params.reset_all();

    assert!(params.get("work_mem").is_none());
    assert_eq!(params.get(ROLE), Some(&"reporting".into()));
    assert_eq!(params.get(SESSION_AUTHORIZATION), Some(&"delegated".into()));
}

#[test]
fn test_reset_all_in_transaction_keeps_identity() {
    let mut params = ParameterStore::default();
    params.begin();
    params.set(ROLE, "reporting");
    params.set("work_mem", "1MB");

    params.reset_all();

    assert!(params.get("work_mem").is_none());
    assert_eq!(params.get(ROLE), Some(&"reporting".into()));
    params.commit();
    assert_eq!(params.get(ROLE), Some(&"reporting".into()));
}

#[test]
fn test_restore_startup_keep_identity() {
    let mut params = ParameterStore::new(Parameters::from(
        [("application_name", "startup")].as_slice(),
    ));
    params.set("application_name", "changed");
    params.set(ROLE, "reporting");

    params.restore_startup_keep_identity();
    assert_eq!(params.get("application_name"), Some(&"startup".into()));
    assert_eq!(params.get(ROLE), Some(&"reporting".into()));

    // DISCARD ALL resets everything.
    params.restore_startup();
    assert!(params.get(ROLE).is_none());
}

#[test]
fn test_server_reset_keep_identity() {
    let mut server = ParameterStore::default();
    let mut client = ParameterStore::default();
    client.set(ROLE, "reporting");
    client.set("work_mem", "1MB");
    server.sync_complete(server.clone().sync(&client));

    server.server_reset_keep_identity();

    // The server still has the role, so a client without one must reset it.
    let peer = ParameterStore::default();
    assert_eq!(queries(server.sync(&peer)), [r#"RESET "role""#]);

    server.server_reset();
    assert!(server.sync(&peer).is_empty());
}

#[test]
fn test_clear_role() {
    let mut params = ParameterStore::default();
    params.set(ROLE, "reporting");
    params.clear_role(false);
    assert!(params.get(ROLE).is_none());

    params.set(ROLE, "reporting");
    params.begin();
    params.clear_role(true);
    assert_eq!(params.get(ROLE), Some(&"none".into()));
    params.commit();
    assert_eq!(params.get(ROLE), Some(&"reporting".into()));

    params.begin();
    params.clear_role(false);
    assert!(params.get(ROLE).is_none());
    params.rollback();
    assert_eq!(params.get(ROLE), Some(&"reporting".into()));
}
