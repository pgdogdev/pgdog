use std::time::Duration;

use tokio::time::timeout;

use super::prelude::*;
use crate::{
    expect_message,
    net::{CommandComplete, DataRow, ErrorResponse, ReadyForQuery, RowDescription},
};

/// Regression for https://github.com/pgdogdev/pgdog/issues/1795.
#[tokio::test]
async fn test_unsupported_aggregation_keeps_connection_usable() {
    let mut client = SpawnedClient::new_sharded(Parameters::default()).await;

    for begin in [None, Some("BEGIN"), Some("BEGIN READ ONLY")] {
        timeout(Duration::from_secs(5), async {
            if let Some(begin) = begin {
                client.send(Query::new(begin)).await;
                assert_eq!(
                    expect_message!(client.read().await, CommandComplete).command(),
                    "BEGIN"
                );
                assert_eq!(
                    expect_message!(client.read().await, ReadyForQuery).status,
                    'T'
                );
            }

            client
                .send(Query::new("SELECT count(DISTINCT value) FROM sharded"))
                .await;

            let messages = timeout(Duration::from_secs(2), client.read_until('E'))
                .await
                .expect("unsupported aggregation did not return an error");
            let error = expect_message!(
                messages.last().expect("error response").clone(),
                ErrorResponse
            );
            assert_eq!(error.severity, "ERROR");
            assert_eq!(error.code, "0A000");
            assert_eq!(
                error.message,
                "unsupported aggregation count: count(DISTINCT ...) is not yet supported"
            );
            assert_eq!(
                expect_message!(
                    timeout(Duration::from_secs(2), client.read())
                        .await
                        .expect("ReadyForQuery missing after unsupported aggregation"),
                    ReadyForQuery
                )
                .status,
                if begin.is_some() { 'E' } else { 'I' }
            );

            if begin.is_some() {
                // The failed transaction must reject further queries until rollback.
                for _ in 0..2 {
                    client
                        .send(Query::new("SELECT count(*) FROM sharded WHERE false"))
                        .await;
                    let error = expect_message!(client.read().await, ErrorResponse);
                    assert_eq!(error.severity, "ERROR");
                    assert_eq!(error.code, "25P02");
                    assert_eq!(
                        expect_message!(client.read().await, ReadyForQuery).status,
                        'E'
                    );
                }

                client.send(Query::new("ROLLBACK")).await;
                assert_eq!(
                    expect_message!(client.read().await, CommandComplete).command(),
                    "ROLLBACK"
                );
                assert_eq!(
                    expect_message!(client.read().await, ReadyForQuery).status,
                    'I'
                );
            }

            // Each shard returns a count row, exercising aggregation again without fixtures.
            client
                .send(Query::new("SELECT count(*) FROM sharded WHERE false"))
                .await;
            expect_message!(client.read().await, RowDescription);
            let row = expect_message!(client.read().await, DataRow);
            assert_eq!(row.get_int(0, true), Some(0));
            assert_eq!(
                expect_message!(client.read().await, CommandComplete).command(),
                "SELECT 1"
            );
            assert_eq!(
                expect_message!(client.read().await, ReadyForQuery).status,
                'I'
            );
        })
        .await
        .unwrap_or_else(|_| panic!("unsupported aggregation or subsequent query hung: {begin:?}"));
    }
}
