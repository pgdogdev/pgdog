use super::prelude::*;

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
