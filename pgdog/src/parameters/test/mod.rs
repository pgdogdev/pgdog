use std::sync::Arc;

use crate::{
    net::{Parameter, Query},
    parameters::*,
};

#[test]
fn test_set_sync() {
    let mut server = ParameterStore::new(Parameters::from(
        [
            ("application_name", "PgDog"),
            ("lock_timeout", "125"),
            ("intervalstyle", "iso_8601"),
        ]
        .as_slice(),
    ));

    let mut client = ParameterStore::new(Parameters::from(
        [
            ("application_name", "test_set"),
            ("intervalstyle", "iso_8601"),
        ]
        .as_slice(),
    ));

    client.begin();
    client.set("application_name", "in_transaction");
    client.set("statement_timeout", "0");
    client.set_local("timezone", "UTC");

    let queries = server.sync(&client);

    assert_eq!(
        queries,
        vec![
            Query::new("RESET \"lock_timeout\""),
            Query::new(r#"SET "application_name" TO "test_set""#)
        ]
    );

    server.sync_complete(queries);

    assert_eq!(
        server.transaction_sync(),
        vec![
            Query::new("SET \"application_name\" TO \"in_transaction\""),
            Query::new("SET \"statement_timeout\" TO \"0\""),
            Query::new("SET LOCAL \"timezone\" TO \"UTC\""),
        ]
    );

    let mut committed = client.clone();
    committed.commit();

    assert_eq!(
        committed.get("application_name").unwrap().as_str().unwrap(),
        "in_transaction",
    );
    assert_eq!(
        committed
            .get("statement_timeout")
            .unwrap()
            .as_str()
            .unwrap(),
        "0"
    );

    assert!(committed.get("timezone").is_none());

    client.rollback();

    assert_eq!(
        client.get("application_name").unwrap().as_str().unwrap(),
        "test_set"
    );
    assert!(client.get("statement_timeout").is_none());

    assert!(server.get("lock_timeout").is_none());

    assert!(server.sync(&client).is_empty());
}

#[test]
fn test_sync_matching_session_params_preserves_transaction_state() {
    let startup = Parameters::from([("DateStyle", "ISO, MDY")].as_slice());
    let mut server = ParameterStore::new(startup.clone());
    let mut client = ParameterStore::new(startup);
    client.begin();
    client.set("statement_timeout", "100");
    client.set_local("timezone", "UTC");

    let sync = server.sync(&client);
    assert!(sync.is_empty());
    server.sync_complete(sync);

    assert_parameter(&server, "statement_timeout", Some("100"));
    assert_parameter(&server, "timezone", Some("UTC"));
    assert_eq!(server.transaction_sync(), client.transaction_sync());

    // The transaction flag must also be copied so this SET rolls back.
    server.set("lock_timeout", "50");
    server.rollback();
    assert_parameter(&server, "lock_timeout", None);
    assert_parameter(&server, "statement_timeout", None);
    assert_parameter(&server, "timezone", None);
    assert_parameter(&server, "DateStyle", Some("ISO, MDY"));
}

#[test]
fn test_commit() {
    let mut store = ParameterStore::new(Parameters::from(
        [
            ("application_name", "startup"),
            ("timezone", "UTC"),
            ("intervalstyle", "iso_8601"),
        ]
        .as_slice(),
    ));

    store.begin();
    store.set("application_name", "committed");
    store.set("statement_timeout", "100");
    store.set_local("timezone", "Europe/Paris");
    store.set_local("lock_timeout", "50");

    assert_parameter(&store, "application_name", Some("committed"));
    assert_parameter(&store, "statement_timeout", Some("100"));
    assert_parameter(&store, "timezone", Some("Europe/Paris"));
    assert_parameter(&store, "lock_timeout", Some("50"));

    assert!(store.commit());

    assert_parameter(&store, "application_name", Some("committed"));
    assert_parameter(&store, "statement_timeout", Some("100"));
    assert_parameter(&store, "timezone", Some("UTC"));
    assert_parameter(&store, "lock_timeout", None);
    assert_parameter(&store, "intervalstyle", Some("iso_8601"));

    // SET after COMMIT is session-scoped and survives the next rollback.
    store.set("application_name", "after_commit");
    store.begin();
    store.set("application_name", "rolled_back");
    store.rollback();
    assert_parameter(&store, "application_name", Some("after_commit"));
    assert_parameter(&store, "statement_timeout", Some("100"));
}

#[test]
fn test_rollback() {
    let mut store = ParameterStore::new(Parameters::from(
        [("application_name", "startup"), ("timezone", "UTC")].as_slice(),
    ));
    store.set("application_name", "before_transaction");
    store.set("statement_timeout", "100");

    store.begin();
    store.set("application_name", "rolled_back");
    store.set("lock_timeout", "50");
    store.set_local("timezone", "Europe/Paris");
    store.set_local("idle_in_transaction_session_timeout", "200");

    assert_parameter(&store, "application_name", Some("rolled_back"));
    assert_parameter(&store, "lock_timeout", Some("50"));
    assert_parameter(&store, "timezone", Some("Europe/Paris"));
    assert_parameter(&store, "idle_in_transaction_session_timeout", Some("200"));

    store.rollback();

    assert_parameter(&store, "application_name", Some("before_transaction"));
    assert_parameter(&store, "statement_timeout", Some("100"));
    assert_parameter(&store, "timezone", Some("UTC"));
    assert_parameter(&store, "lock_timeout", None);
    assert_parameter(&store, "idle_in_transaction_session_timeout", None);

    // A later commit must not resurrect any rolled-back values.
    store.set("application_name", "after_rollback");
    store.begin();
    store.commit();
    assert_parameter(&store, "application_name", Some("after_rollback"));
    assert_parameter(&store, "lock_timeout", None);
    assert_parameter(&store, "idle_in_transaction_session_timeout", None);
}

#[test]
fn test_reset() {
    for in_transaction in [false, true] {
        let mut store = ParameterStore::new(Parameters::from(
            [("application_name", "startup"), ("timezone", "UTC")].as_slice(),
        ));
        store.set("application_name", "session");
        store.set("statement_timeout", "100");

        if in_transaction {
            store.begin();
            store.set("application_name", "transaction");
            store.set_local("statement_timeout", "200");
            store.set("lock_timeout", "50");
            store.set_local("idle_in_transaction_session_timeout", "300");
        }

        store.reset("application_name");
        store.reset("statement_timeout");
        store.reset("lock_timeout");
        store.reset("idle_in_transaction_session_timeout");
        store.reset("missing_parameter");

        assert_parameter(&store, "application_name", Some("startup"));
        assert_parameter(&store, "statement_timeout", None);
        assert_parameter(&store, "lock_timeout", None);
        assert_parameter(&store, "idle_in_transaction_session_timeout", None);
        assert_parameter(&store, "missing_parameter", None);
        assert_parameter(&store, "timezone", Some("UTC"));

        let mut committed = store.clone();
        committed.commit();
        assert_parameter(&committed, "application_name", Some("startup"));
        assert_parameter(&committed, "statement_timeout", None);
        assert_parameter(&committed, "lock_timeout", None);
        assert_parameter(&committed, "idle_in_transaction_session_timeout", None);
        // A subsequent rollback must not undo an already committed reset.
        committed.begin();
        committed.rollback();
        assert_parameter(&committed, "application_name", Some("startup"));
        assert_parameter(&committed, "statement_timeout", None);

        store.rollback();
        assert_parameter(
            &store,
            "application_name",
            Some(if in_transaction { "session" } else { "startup" }),
        );
        assert_parameter(
            &store,
            "statement_timeout",
            if in_transaction { Some("100") } else { None },
        );
        assert_parameter(&store, "lock_timeout", None);
        assert_parameter(&store, "idle_in_transaction_session_timeout", None);
        assert_parameter(&store, "timezone", Some("UTC"));
    }
}

#[test]
fn test_reset_all() {
    for in_transaction in [false, true] {
        let mut store = ParameterStore::new(Parameters::from(
            [
                ("application_name", "startup"),
                ("timezone", "UTC"),
                ("client_encoding", "UTF8"),
            ]
            .as_slice(),
        ));
        store.set("application_name", "session");
        store.set("statement_timeout", "100");
        store.set("client_encoding", "LATIN1");
        store.set("pgdog.shard", "1");

        if in_transaction {
            store.begin();
            store.set("application_name", "transaction");
            store.set_local("timezone", "Europe/Paris");
            store.set("lock_timeout", "50");
            store.set_local("idle_in_transaction_session_timeout", "300");
        }

        store.reset_all();

        assert_parameter(&store, "application_name", Some("startup"));
        assert_parameter(&store, "timezone", Some("UTC"));
        assert_parameter(&store, "statement_timeout", None);
        assert_parameter(&store, "lock_timeout", None);
        assert_parameter(&store, "idle_in_transaction_session_timeout", None);
        assert_parameter(&store, "client_encoding", Some("LATIN1"));
        assert_parameter(&store, "pgdog.shard", Some("1"));

        let mut committed = store.clone();
        committed.commit();
        assert_parameter(&committed, "application_name", Some("startup"));
        assert_parameter(&committed, "timezone", Some("UTC"));
        assert_parameter(&committed, "statement_timeout", None);
        assert_parameter(&committed, "lock_timeout", None);
        assert_parameter(&committed, "idle_in_transaction_session_timeout", None);
        assert_parameter(&committed, "client_encoding", Some("LATIN1"));
        assert_parameter(&committed, "pgdog.shard", Some("1"));

        store.rollback();
        assert_parameter(
            &store,
            "application_name",
            Some(if in_transaction { "session" } else { "startup" }),
        );
        assert_parameter(
            &store,
            "statement_timeout",
            if in_transaction { Some("100") } else { None },
        );
        assert_parameter(&store, "timezone", Some("UTC"));
        assert_parameter(&store, "lock_timeout", None);
        assert_parameter(&store, "idle_in_transaction_session_timeout", None);
        assert_parameter(&store, "client_encoding", Some("LATIN1"));
        assert_parameter(&store, "pgdog.shard", Some("1"));
    }
}

#[test]
fn test_set_local_preserves_transaction_set() {
    let mut store = ParameterStore::new(Parameters::from(
        [("application_name", "startup")].as_slice(),
    ));
    store.set("application_name", "session");
    store.begin();
    store.set("application_name", "transaction");
    store.set_local("application_name", "local");
    assert_parameter(&store, "application_name", Some("local"));

    let mut rolled_back = store.clone();
    rolled_back.rollback();
    assert_parameter(&rolled_back, "application_name", Some("session"));

    assert!(store.commit());
    assert_parameter(&store, "application_name", Some("transaction"));
}

#[test]
fn test_set_overrides_set_local() {
    let mut store = ParameterStore::default();
    store.begin();
    store.set_local("application_name", "local");
    store.set("application_name", "transaction");
    assert_parameter(&store, "application_name", Some("transaction"));

    assert!(store.commit());
    assert_parameter(&store, "application_name", Some("transaction"));
}

#[test]
fn test_commit_without_session_changes() {
    for local in [false, true] {
        let mut store = ParameterStore::new(Parameters::from(
            [("application_name", "startup")].as_slice(),
        ));
        store.begin();
        if local {
            store.set_local("application_name", "local");
            assert_parameter(&store, "application_name", Some("local"));
        }

        assert!(!store.commit());
        assert_parameter(&store, "application_name", Some("startup"));
    }
}

#[test]
fn test_commit_reports_reset_only_changes() {
    for reset_all in [false, true] {
        let mut store = ParameterStore::new(Parameters::from(
            [("application_name", "startup")].as_slice(),
        ));
        store.set("application_name", "session");
        store.begin();
        if reset_all {
            store.reset_all();
        } else {
            store.reset("application_name");
        }

        assert_parameter(&store, "application_name", Some("startup"));
        assert!(store.commit());
        assert_parameter(&store, "application_name", Some("startup"));

        // Committed resets must not be reported again in a later transaction.
        store.begin();
        assert!(!store.commit());
    }
}

#[test]
fn test_reset_mixed_case_restores_startup() {
    for in_transaction in [false, true] {
        let mut store = ParameterStore::new(Parameters::from([("TimeZone", "UTC")].as_slice()));
        store.set("TimeZone", "Europe/Paris");
        if in_transaction {
            store.begin();
            store.set("TimeZone", "America/New_York");
            store.set_local("TimeZone", "Asia/Tokyo");
        }

        store.reset("TimeZone");
        assert_parameter(&store, "timezone", Some("UTC"));

        let mut committed = store.clone();
        committed.commit();
        assert_parameter(&committed, "timezone", Some("UTC"));

        store.rollback();
        assert_parameter(
            &store,
            "timezone",
            Some(if in_transaction {
                "Europe/Paris"
            } else {
                "UTC"
            }),
        );
    }
}

#[test]
fn test_repeated_resets_preserve_rollback_values() {
    for reset_all in [false, true] {
        let mut store = ParameterStore::new(Parameters::from(
            [("application_name", "startup")].as_slice(),
        ));
        store.set("application_name", "session");
        store.set("statement_timeout", "100");
        store.begin();

        for _ in 0..2 {
            if reset_all {
                store.reset_all();
            } else {
                store.reset("application_name");
                store.reset("statement_timeout");
            }
            assert_parameter(&store, "application_name", Some("startup"));
            assert_parameter(&store, "statement_timeout", None);
        }

        assert!(store.rollback());
        assert_parameter(&store, "application_name", Some("session"));
        assert_parameter(&store, "statement_timeout", Some("100"));

        store.begin();
        assert!(!store.commit());
        assert_parameter(&store, "application_name", Some("session"));
        assert_parameter(&store, "statement_timeout", Some("100"));
    }
}

fn assert_parameter(store: &ParameterStore, name: &str, expected: Option<&str>) {
    assert_eq!(
        store.get(name),
        expected.map(ParameterValue::from).as_ref(),
        "unexpected value for {name}"
    );
}

#[test]
fn test_startup_normalizes_names_and_hashes() {
    let startup = Parameters::new([
        ("TimeZone".into(), "UTC".into()),
        ("timezone".into(), "Europe/Paris".into()),
        ("user".into(), "alice".into()),
    ]);
    let expected = Parameters::from([("timezone", "Europe/Paris")].as_slice());
    assert_eq!(startup.get("timezone"), expected.get("timezone"));
    assert!(startup.identical(&expected));
    assert_eq!(startup.len(), 2);

    let client = ParameterStore::new(startup);
    let mut server = ParameterStore::default();
    let sync = server.sync(&client);
    assert_eq!(
        sync,
        vec![Query::new(r#"SET "timezone" TO "Europe/Paris""#)]
    );
    server.sync_complete(sync);
    assert!(server.sync(&client).is_empty());
}

#[test]
fn test_restore_startup_discards_all_runtime_state() {
    let startup = Parameters::from([("application_name", "startup")].as_slice());
    let mut store = ParameterStore::new(startup.clone());
    store.set("application_name", "session");
    store.set("pgdog.shard", "1");
    store.begin();
    store.reset("application_name");
    store.set("statement_timeout", "100");
    store.set_local("timezone", "UTC");
    store.restore_startup();
    store.rollback();
    assert_eq!(&*store, &startup);
    assert!(store.transaction_sync().is_empty());
    store.set("application_name", "new_session");
    store.rollback();
    assert_parameter(&store, "application_name", Some("new_session"));
}

fn startup_with_server_datestyle(server: &str, client: Option<&str>) -> Parameters {
    let mut startup = Parameters::default();
    if let Some(client) = client {
        startup.insert("datestyle", client);
    }
    startup.set_server_defaults(&Parameters::from([("DateStyle", server)].as_slice()));
    startup
}

#[test]
fn test_startup_datestyle_merges_with_server_datestyle() {
    for (client, expected) in [
        (None, "SQL, DMY"),
        (Some("YMD"), "SQL, YMD"),
        (Some("iso"), "ISO, DMY"),
        (Some("german"), "German, DMY"),
        (Some("iso,mdy"), "ISO, MDY"),
        (Some("ISO, MDY"), "ISO, MDY"),
    ] {
        let startup = startup_with_server_datestyle("SQL, DMY", client);
        assert_eq!(
            startup.get("DateStyle").and_then(ParameterValue::as_str),
            Some(expected),
            "startup DateStyle {client:?}"
        );
    }

    let mut client = Parameters::from([("DateStyle", "YMD")].as_slice());
    client.set_server_defaults(&Parameters::default());
    assert_eq!(
        client.get("DateStyle").and_then(ParameterValue::as_str),
        Some("YMD"),
        "no server DateStyle leaves the client value unchanged"
    );
}

#[test]
fn test_invalid_startup_datestyle_is_kept_unlike_postgres() {
    let startup = startup_with_server_datestyle("SQL, DMY", Some("bogus"));
    assert_eq!(
        startup.get("DateStyle").and_then(ParameterValue::as_str),
        Some("bogus")
    );
}

#[test]
fn test_reset_datestyle_restores_normalized_startup_value() {
    for in_transaction in [false, true] {
        let mut store =
            ParameterStore::new(startup_with_server_datestyle("ISO, MDY", Some("sql,dmy")));
        if in_transaction {
            store.begin();
        }
        store.set("DateStyle", "Postgres, MDY");
        store.reset("DateStyle");
        assert_parameter(&store, "DateStyle", Some("SQL, DMY"));
    }
}

#[test]
fn test_partial_startup_datestyle_syncs_full_value() {
    let mut server = ParameterStore::new(Parameters::from([("DateStyle", "ISO, MDY")].as_slice()));
    let previous = ParameterStore::new(startup_with_server_datestyle("ISO, MDY", Some("sql,dmy")));
    let queries = server.sync(&previous);
    server.sync_complete(queries);

    let client = ParameterStore::new(startup_with_server_datestyle("ISO, MDY", Some("YMD")));
    assert_eq!(
        server.sync(&client),
        vec![Query::new(r#"SET "datestyle" TO "ISO, YMD""#)],
        "a partial startup DateStyle must not keep the previous client's style"
    );
}

#[test]
fn test_reset_application_name_reports_server_name() {
    let server_params = Parameters::from([("application_name", "PgDog")].as_slice());
    let mut startup = Parameters::default();
    startup.set_server_defaults(&server_params);
    let mut client = ParameterStore::new(startup).with_server_reported(Arc::new(server_params));
    let mut server = ParameterStore::default();

    assert!(
        server.sync(&client).is_empty(),
        "no SET for a client without a name"
    );

    client.set("application_name", "myapp");
    let queries = server.sync(&client);
    server.sync_complete(queries);
    client.reset("application_name");

    assert_eq!(
        client
            .get_reported("application_name")
            .and_then(ParameterValue::as_str),
        Some("PgDog"),
        "the client sees the name the server connection really uses"
    );
    assert_eq!(
        server.sync(&client),
        vec![Query::new(r#"RESET "application_name""#)]
    );
}

fn store_with_server_timezone(startup: &[(&str, &str)]) -> ParameterStore {
    ParameterStore::new(Parameters::from(startup))
        .with_server_reported(Arc::new(Parameters::from([("TimeZone", "UTC")].as_slice())))
}

fn reported_timezone(store: &ParameterStore) -> Option<&str> {
    store
        .get_reported("TimeZone")
        .and_then(ParameterValue::as_str)
}

#[test]
fn test_get_reported_falls_back_to_server() {
    let mut store = store_with_server_timezone(&[]);
    assert_eq!(reported_timezone(&store), Some("UTC"));

    store.set("TimeZone", "Europe/Paris");
    assert_eq!(reported_timezone(&store), Some("Europe/Paris"));

    store.reset("TimeZone");
    assert_eq!(
        reported_timezone(&store),
        Some("UTC"),
        "RESET without a startup value restores the server value"
    );
}

#[test]
fn test_get_reported_prefers_startup_over_server() {
    let mut store = store_with_server_timezone(&[("TimeZone", "Europe/Paris")]);
    store.set("TimeZone", "UTC");
    store.reset("TimeZone");
    assert_eq!(reported_timezone(&store), Some("Europe/Paris"));
}

#[test]
fn test_set_to_server_value_is_not_reported() {
    let store = store_with_server_timezone(&[]);
    let utc = ParameterValue::from("UTC");
    assert!(
        ParameterValidator::notify_client("TimeZone", store.get_reported("TimeZone"), Some(&utc))
            .is_none()
    );
}

#[test]
fn test_rollback_reports_server_value() {
    let mut store = store_with_server_timezone(&[]);
    store.begin();
    store.set("TimeZone", "Europe/Paris");

    let statuses: Vec<_> = store.transaction_end_status(true).collect();
    assert_eq!(statuses.len(), 1);
    assert_eq!(statuses[0].name, "TimeZone");
    assert_eq!(statuses[0].value.as_str(), Some("UTC"));

    store.rollback();
    assert_eq!(reported_timezone(&store), Some("UTC"));
}

#[test]
fn test_commit_reports_nothing_when_value_kept() {
    let mut store = store_with_server_timezone(&[]);
    store.begin();
    store.set("TimeZone", "Europe/Paris");
    assert_eq!(store.transaction_end_status(false).count(), 0);
}

#[test]
fn test_commit_after_set_local_reports_server_value() {
    let mut store = store_with_server_timezone(&[]);
    store.begin();
    store.set_local("TimeZone", "Europe/Paris");

    let statuses: Vec<_> = store.transaction_end_status(false).collect();
    assert_eq!(statuses.len(), 1);
    assert_eq!(statuses[0].name, "TimeZone");
    assert_eq!(statuses[0].value.as_str(), Some("UTC"));

    store.commit();
    assert_eq!(reported_timezone(&store), Some("UTC"));
}

#[test]
fn test_commit_after_reset_in_transaction_reports_nothing() {
    let mut store = store_with_server_timezone(&[]);
    store.set("TimeZone", "Europe/Paris");
    store.begin();
    store.reset("TimeZone");

    assert_eq!(reported_timezone(&store), Some("UTC"));
    assert_eq!(store.transaction_end_status(false).count(), 0);

    store.commit();
    assert_eq!(reported_timezone(&store), Some("UTC"));
}

mod server;
mod value;

pub(crate) fn new_test(name: &str) -> Parameters {
    Parameters::from(vec![
        Parameter::from(("application_name", name)),
        Parameter::from(("pgdog.test_mode", "1")),
    ])
}
