use super::prelude::*;
use crate::net::{CommandComplete, DataRow, ParameterStatus};

#[tokio::test]
async fn multiple_set_and_reset_report_each_change() {
    let mut startup = Parameters::default();
    startup.insert("application_name", "startup");
    startup.insert("timezone", "UTC");
    let mut client = TestClient::new_sharded(startup).await;
    client
        .send_simple(Query::new(
            "SET application_name = 'first';
             SET application_name = 'first';
             SET timezone = 'Europe/Paris';
             RESET application_name;
             SET work_mem = '4MB';
             SET application_name = 'last'",
        ))
        .await;
    let messages = client.read_until('Z').await.expect("SET batch completed");
    assert_eq!(
        messages
            .iter()
            .map(|message| message.code())
            .collect::<Vec<_>>(),
        ['C', 'S', 'C', 'C', 'S', 'C', 'S', 'C', 'C', 'S', 'Z'],
    );
    let commands = messages
        .iter()
        .filter(|message| message.code() == 'C')
        .map(|message| {
            CommandComplete::try_from(message.clone())
                .expect("command completion")
                .command()
                .to_owned()
        })
        .collect::<Vec<_>>();
    assert_eq!(commands, ["SET", "SET", "SET", "RESET", "SET", "SET"]);
    let statuses = messages
        .iter()
        .filter(|message| message.code() == 'S')
        .map(|message| ParameterStatus::try_from(message.clone()).expect("parameter status"))
        .collect::<Vec<_>>();
    for (status, (name, value)) in statuses.iter().zip([
        ("application_name", "first"),
        ("TimeZone", "Europe/Paris"),
        ("application_name", "startup"),
        ("application_name", "last"),
    ]) {
        assert_eq!(status.name, name);
        assert_eq!(status.value.as_str(), Some(value));
    }
    assert_eq!(
        client.client().params.get("application_name"),
        Some(&"last".into())
    );
    assert!(!client.backend_connected());

    // Repeating the effective value sends no status and leaves no extra RFQ.
    client
        .send_simple(Query::new("SET application_name = 'last'"))
        .await;
    let messages = client
        .read_until('Z')
        .await
        .expect("unchanged SET completed");
    assert_eq!(
        messages
            .iter()
            .map(|message| message.code())
            .collect::<Vec<_>>(),
        ['C', 'Z']
    );
}

#[tokio::test]
async fn multiple_set_local_outside_transaction_warn_per_command() {
    let mut client = TestClient::new_sharded(Parameters::default()).await;
    client
        .send_simple(Query::new(
            "SET work_mem = '4MB';
         SET LOCAL application_name = 'ignored';
         SET LOCAL timezone = 'Europe/Paris';
         SET lock_timeout = '1s'",
        ))
        .await;
    let messages = client.read_until('Z').await.expect("SET batch completed");
    assert_eq!(
        messages
            .iter()
            .map(|message| message.code())
            .collect::<Vec<_>>(),
        ['C', 'N', 'C', 'N', 'C', 'C', 'Z']
    );
    assert_eq!(client.client().params.get("application_name"), None);
    assert_eq!(client.client().params.get("timezone"), None);
    assert_eq!(client.client().params.get("work_mem"), Some(&"4MB".into()));
    assert_eq!(
        client.client().params.get("lock_timeout"),
        Some(&"1s".into())
    );
    assert!(!client.backend_connected());
}

#[tokio::test]
async fn set_config_returns_row_and_parameter_status() {
    for extended in [false, true] {
        let mut startup = Parameters::default();
        startup.insert("application_name", "startup");
        let mut client = TestClient::new_sharded(startup).await;
        for (argument, result, status) in [
            ("'changed'", Some("changed"), "changed"),
            ("NULL", None, "startup"),
        ] {
            let query = format!("SELECT set_config('application_name', {argument}, false)");
            if extended {
                client.send(Parse::new_anonymous(&query)).await;
                client.send(Bind::new_statement("")).await;
                client.send(Describe::new_statement("")).await;
                client.send(Execute::new()).await;
                client.send(Sync).await;
                client.try_process().await.expect("set_config processed");
            } else {
                client.send_simple(Query::new(&query)).await;
            }
            let messages = client.read_until('Z').await.expect("set_config completed");
            let expected = if extended {
                vec!['1', '2', 't', 'T', 'D', 'C', 'S', 'Z']
            } else {
                vec!['T', 'D', 'C', 'S', 'Z']
            };
            assert_eq!(
                messages
                    .iter()
                    .map(|message| message.code())
                    .collect::<Vec<_>>(),
                expected
            );
            let row = messages
                .iter()
                .find(|message| message.code() == 'D')
                .expect("row");
            let row = DataRow::try_from(row.clone()).expect("data row");
            assert_eq!(row.get_raw(0).expect("column").is_null, result.is_none());
            if let Some(result) = result {
                assert_eq!(row.get_text(0).as_deref(), Some(result));
            }
            let command = messages
                .iter()
                .find(|message| message.code() == 'C')
                .expect("command");
            assert_eq!(
                CommandComplete::try_from(command.clone())
                    .expect("completion")
                    .command(),
                "SELECT 1"
            );
            let message = messages
                .iter()
                .find(|message| message.code() == 'S')
                .expect("status");
            let parameter = ParameterStatus::try_from(message.clone()).expect("parameter status");
            assert_eq!(parameter.name, "application_name");
            assert_eq!(parameter.value.as_str(), Some(status));
            assert!(!client.backend_connected());
        }
    }
}

#[tokio::test]
async fn datestyle_batch_uses_each_preceding_value_and_reset() {
    let mut startup = Parameters::default();
    startup.insert("DateStyle", "ISO, YMD");
    let mut client = TestClient::new_sharded(startup).await;
    client
        .send_simple(Query::new(
            "SET datestyle = SQL, YMD;
             SET datestyle = 'MDY';
             SET datestyle = 'SQL';
             RESET datestyle;
             SET datestyle = 'MDY'",
        ))
        .await;
    let messages = client.read_until('Z').await.expect("DateStyle batch");
    assert_eq!(
        messages
            .iter()
            .map(|message| message.code())
            .collect::<Vec<_>>(),
        ['C', 'S', 'C', 'S', 'C', 'C', 'S', 'C', 'S', 'Z']
    );
    let statuses = messages
        .iter()
        .filter(|message| message.code() == 'S')
        .map(|message| ParameterStatus::try_from(message.clone()).expect("status"))
        .collect::<Vec<_>>();
    for (status, expected) in statuses
        .iter()
        .zip(["SQL, YMD", "SQL, MDY", "ISO, YMD", "ISO, MDY"])
    {
        assert_eq!(status.name, "DateStyle");
        assert_eq!(status.value.as_str(), Some(expected));
    }
    assert_eq!(
        client.client().params.get_default("datestyle", ""),
        "ISO, MDY"
    );
    assert!(!client.backend_connected());
}

#[tokio::test]
async fn datestyle_set_config_returns_normalized_value() {
    for extended in [false, true] {
        let mut startup = Parameters::default();
        startup.insert("DateStyle", "SQL, YMD");
        let mut client = TestClient::new_sharded(startup).await;
        let query = "SELECT set_config('DateStyle', 'MDY', false)";
        if extended {
            client.send(Parse::new_anonymous(query)).await;
            client.send(Bind::new_statement("")).await;
            client.send(Execute::new()).await;
            client.send(Sync).await;
            client.try_process().await.expect("set_config processed");
        } else {
            client.send_simple(Query::new(query)).await;
        }
        let messages = client.read_until('Z').await.expect("set_config completed");
        let row = messages
            .iter()
            .find(|message| message.code() == 'D')
            .expect("row");
        assert_eq!(
            DataRow::try_from(row.clone())
                .expect("data row")
                .get_text(0)
                .as_deref(),
            Some("SQL, MDY")
        );
        let status = messages
            .iter()
            .find(|message| message.code() == 'S')
            .expect("status");
        assert_eq!(
            ParameterStatus::try_from(status.clone())
                .expect("parameter status")
                .value
                .as_str(),
            Some("SQL, MDY")
        );
        assert_eq!(
            client.client().params.get_default("datestyle", ""),
            "SQL, MDY"
        );
        assert!(!client.backend_connected());
    }
}

#[tokio::test]
async fn datestyle_local_inheritance_and_transaction_end() {
    for (end, expected) in [("ROLLBACK", "ISO, MDY"), ("COMMIT", "SQL, DMY")] {
        let mut startup = Parameters::default();
        startup.insert("DateStyle", "ISO, MDY");
        let mut client = TestClient::new_sharded(startup).await;
        for query in [
            "BEGIN",
            "SET LOCAL datestyle = 'SQL'",
            "SET datestyle = 'DMY'",
        ] {
            client.send_simple(Query::new(query)).await;
            client.read_until('Z').await.expect("SET completed");
        }
        assert_eq!(
            client.client().params.get_default("DateStyle", ""),
            "SQL, DMY"
        );
        client.send_simple(Query::new(end)).await;
        client.read_until('Z').await.expect("transaction ended");
        assert_eq!(
            client.client().params.get_default("DateStyle", ""),
            expected
        );
        client
            .send_simple(Query::new("SET datestyle = 'DEFAULT, YMD'"))
            .await;
        client.read_until('Z').await.expect("reset format");
        assert_eq!(
            client.client().params.get_default("DateStyle", ""),
            "ISO, YMD"
        );
    }
}

#[tokio::test]
async fn invalid_datestyle_batch_preserves_state_and_aborts_transaction() {
    use crate::{
        expect_message,
        net::{ErrorResponse, ReadyForQuery},
    };

    for in_transaction in [false, true] {
        let mut startup = Parameters::default();
        startup.insert("DateStyle", "ISO, MDY");
        let mut client = TestClient::new_sharded(startup).await;
        if in_transaction {
            client.send_simple(Query::new("BEGIN")).await;
            client.read_until('Z').await.expect("transaction started");
        }
        client
            .send_simple(Query::new(
                "SET datestyle = 'SQL'; SET datestyle = 'ISO, SQL'",
            ))
            .await;
        let error = expect_message!(client.read().await, ErrorResponse);
        assert_eq!(error.code, "22023");
        assert_eq!(
            error.message,
            "invalid value for parameter \"DateStyle\": \"ISO, SQL\""
        );
        assert_eq!(
            error.detail.as_deref(),
            Some("Conflicting \"DateStyle\" specifications.")
        );
        assert_eq!(
            expect_message!(client.read().await, ReadyForQuery).status,
            if in_transaction { 'E' } else { 'I' }
        );
        assert_eq!(
            client.client().params.get_default("DateStyle", ""),
            "ISO, MDY"
        );
        assert!(!client.backend_connected());
    }
}

#[tokio::test]
async fn unknown_parameters_pass_through_in_any_naming_format() {
    use crate::net::ReadyForQuery;

    for name in [
        "no_such_variable",
        "app.setting",
        "app..setting",
        "some setting",
        "1bad-name",
    ] {
        for (query, expected) in [
            (format!("SET \"{name}\" TO 42"), Some("42")),
            (format!("SET \"{name}\" TO DEFAULT"), None),
            (format!("RESET \"{name}\""), None),
            (
                format!("SELECT set_config('{name}', '42', false)"),
                Some("42"),
            ),
        ] {
            for extended in [false, true] {
                for in_transaction in [false, true] {
                    let mut client = TestClient::new_sharded(Parameters::default()).await;
                    if in_transaction {
                        client.send_simple(Query::new("BEGIN")).await;
                        client.read_until('Z').await.expect("transaction started");
                    }
                    if extended {
                        client.send(Parse::new_anonymous(&query)).await;
                        client.send(Bind::new_statement("")).await;
                        client.send(Execute::new()).await;
                        client.send(Sync).await;
                        client.try_process().await.expect("unknown SET processed");
                    } else {
                        client.send_simple(Query::new(&query)).await;
                    }
                    let messages = client
                        .read_until('Z')
                        .await
                        .expect("unknown parameter accepted");
                    let ready =
                        ReadyForQuery::try_from(messages.last().expect("ready message").clone())
                            .expect("ready for query");
                    assert_eq!(
                        ready.status,
                        if in_transaction { 'T' } else { 'I' },
                        "{query}"
                    );
                    assert_eq!(
                        client.client().params.get(name),
                        expected.map(Into::into).as_ref(),
                        "{query}"
                    );
                    assert!(!client.backend_connected());
                }
            }
        }
    }

    let mut client = TestClient::new_sharded(Parameters::default()).await;
    client
        .send_simple(Query::new(
            "SET application_name = 'changed'; SET no_such_variable = 42",
        ))
        .await;
    client.read_until('Z').await.expect("SET batch accepted");
    assert_eq!(
        client.client().params.get("application_name"),
        Some(&"changed".into())
    );
    assert_eq!(
        client.client().params.get("no_such_variable"),
        Some(&"42".into())
    );
    assert!(!client.backend_connected());
}
