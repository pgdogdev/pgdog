use std::io::ErrorKind;
use std::time::Duration;

use tokio::time::timeout;

use crate::{
    expect_message,
    net::{BindComplete, CommandComplete, ErrorResponse, Parameters, ParseComplete, ReadyForQuery},
};

use super::prelude::*;

#[tokio::test]
async fn pub_sub_disabled_keeps_connection_usable() {
    let mut client = SpawnedClient::new_sharded(Parameters::default()).await;

    timeout(Duration::from_secs(5), async {
        for (begin, query) in [
            (false, "LISTEN pgdog_disabled"),
            (false, "NOTIFY pgdog_disabled"),
            (true, "LISTEN pgdog_disabled"),
            (true, "NOTIFY pgdog_disabled"),
        ] {
            if begin {
                client.send(Query::new("BEGIN")).await;
                client.read_until('Z').await;
            }

            client.send(Query::new(query)).await;
            let error = expect_message!(client.read().await, ErrorResponse);
            assert_eq!(error.severity, "ERROR", "{query}");
            assert_eq!(error.code, "0A000", "{query}");
            assert_eq!(error.message, "pub/sub disabled", "{query}");
            assert!(
                error
                    .hint
                    .as_deref()
                    .is_some_and(|hint| hint.contains("pub_sub_channel_size")),
                "{query}"
            );
            assert_eq!(
                expect_message!(client.read().await, ReadyForQuery).status,
                if begin { 'E' } else { 'I' },
                "{query}"
            );

            if begin {
                client.send(Query::new("ROLLBACK")).await;
                assert_eq!(
                    expect_message!(client.read().await, CommandComplete).command(),
                    "ROLLBACK"
                );
                expect_message!(client.read().await, ReadyForQuery);
            }
        }

        client.send(Query::new("SELECT 1")).await;
        let messages = client.read_until('Z').await;
        assert!(
            messages.iter().any(|message| message.code() == 'D'),
            "connection still serves queries"
        );
    })
    .await
    .expect("pub/sub disabled error or subsequent query hung");
}

#[tokio::test]
async fn extended_unlisten_flush_does_not_emit_ready_for_query() {
    let mut client = TestClient::new_sharded(Parameters::default()).await;
    client.client.client_request = ClientRequest::from(vec![
        Parse::new_anonymous("UNLISTEN pgdog_pipeline_listen").into(),
        Bind::new_statement("").into(),
        Execute::new().into(),
        Flush.into(),
    ]);

    let (mut context, client_request) = QueryEngineContext::new(&mut client.client);
    client
        .engine
        .unlisten(
            &mut context,
            &client_request.messages,
            "pgdog_pipeline_listen",
        )
        .await
        .unwrap();

    expect_message!(client.read().await, ParseComplete);
    expect_message!(client.read().await, BindComplete);
    assert_eq!(
        expect_message!(client.read().await, CommandComplete).command(),
        "UNLISTEN"
    );

    let mut unexpected = [0];
    let error = client
        .conn
        .try_read(&mut unexpected)
        .expect_err("Flush-only extended response must not include ReadyForQuery");
    assert_eq!(error.kind(), ErrorKind::WouldBlock);
}
