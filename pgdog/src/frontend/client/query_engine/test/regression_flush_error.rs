use std::time::Duration;

use pgdog_config::QueryParserLevel;
use tokio::time::timeout;

use super::prelude::*;
use crate::{
    config::{PoolerMode, load_test_with_pooler_mode},
    expect_message,
    net::{DataRow, ErrorResponse, ReadyForQuery, RowDescription},
};

async fn assert_flush_error_recovers(query: &str, sqlstate: &str, pending: &[ProtocolMessage]) {
    for mode in [PoolerMode::Session, PoolerMode::Transaction] {
        load_test_with_pooler_mode(mode);
        super::change_config(|general| general.query_parser = QueryParserLevel::Off);
        let mut client = SpawnedClient::new(Parameters::default()).await;

        timeout(Duration::from_secs(5), async {
            // pg-cursor submits the portal with Flush, then sends Execute separately.
            client.send(Parse::new_anonymous(query)).await;
            client.send(Bind::new_name_portal("", "cursor")).await;
            client.send(Describe::new_portal("cursor")).await;
            client.send(Flush).await;
            for message in pending {
                client.send(message.clone()).await;
            }

            let replies = client.read_until('E').await;
            let error =
                expect_message!(replies.last().expect("error reply").clone(), ErrorResponse);
            assert_eq!(error.code, sqlstate);

            // A client may already have included Sync with the ignored commands.
            if !pending.iter().any(|message| message.code() == 'S') {
                client.send(Sync).await;
            }
            assert_eq!(
                expect_message!(client.read().await, ReadyForQuery).status,
                'I'
            );

            // Recovery must leave the same client connection usable.
            client.send(Query::new("SELECT 1 AS ok")).await;
            expect_message!(client.read().await, RowDescription);
            let row = expect_message!(client.read().await, DataRow);
            assert_eq!(row.get_int(0, true), Some(1));
            client.read_until('Z').await;
            client.send(Terminate).await;
            client.join().await;
        })
        .await
        .expect("Flush error recovery must consume Sync and allow the next query");
    }
}

#[tokio::test]
async fn test_flush_parse_error_recovers() {
    assert_flush_error_recovers(
        "SELECT no_such_column",
        "42703",
        &[Execute::new_portal_limit("cursor", 1).into(), Flush.into()],
    )
    .await;
}

#[tokio::test]
async fn test_flush_bind_error_recovers() {
    assert_flush_error_recovers(
        "SELECT 1 / 0",
        "22012",
        &[Execute::new_portal_limit("cursor", 1).into(), Flush.into()],
    )
    .await;
}

#[tokio::test]
async fn test_flush_error_preserves_sync_in_ignored_batch() {
    assert_flush_error_recovers(
        "SELECT no_such_column",
        "42703",
        &[
            Parse::new_anonymous("SELECT 2").into(),
            Bind::new_statement("").into(),
            Execute::new().into(),
            Sync.into(),
        ],
    )
    .await;
}

#[tokio::test]
async fn test_flush_error_discards_last_split_request() {
    assert_flush_error_recovers(
        "SELECT no_such_column",
        "42703",
        &[
            Execute::new_portal_limit("cursor", 1).into(),
            Execute::new_portal_limit("cursor", 1).into(),
            Flush.into(),
        ],
    )
    .await;
}
