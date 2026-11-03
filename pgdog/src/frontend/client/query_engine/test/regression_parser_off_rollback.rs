use pgdog_config::QueryParserLevel;

use super::prelude::*;
use crate::{
    config::{PoolerMode, load_test_with_pooler_mode},
    expect_message,
    net::{CommandComplete, DataRow, ReadyForQuery, RowDescription},
};

async fn assert_recovery(recovery: &str, status: char, extended: bool) {
    for mode in [PoolerMode::Session, PoolerMode::Transaction] {
        load_test_with_pooler_mode(mode);
        super::change_config(|general| general.query_parser = QueryParserLevel::Off);
        let mut client = TestClient::new(Parameters::default()).await;

        for sql in ["BEGIN", "SAVEPOINT s"] {
            client.send_simple(Query::new(sql)).await;
            expect_message!(client.read().await, CommandComplete);
            assert_eq!(
                expect_message!(client.read().await, ReadyForQuery).status,
                'T'
            );
        }

        for (sql, code) in [("SELECT 1/0", "22012"), ("SELECT 1", "25P02")] {
            client.send_simple(Query::new(sql)).await;
            let error = client.read_until('Z').await.expect_err("transaction error");
            assert_eq!(error.code, code);
            assert_eq!(
                expect_message!(client.read().await, ReadyForQuery).status,
                'E'
            );
        }

        if extended {
            client.send(Parse::new_anonymous(recovery)).await;
            client.send(Bind::new_statement("")).await;
            client.send(Execute::new()).await;
            client.send(Sync).await;
            client.try_process().await.expect("process recovery");
        } else {
            client.send_simple(Query::new(recovery)).await;
        }
        let messages = client.read_until('Z').await.expect("recovery must succeed");
        assert!(messages.iter().any(|message| message.code() == 'C'));
        assert_eq!(
            expect_message!(
                messages.last().expect("ReadyForQuery").clone(),
                ReadyForQuery
            )
            .status,
            status
        );
        assert_eq!(client.client.transaction.is_some(), status == 'T');
        assert!(
            client
                .client
                .transaction
                .is_none_or(|transaction| !transaction.error())
        );

        client.send_simple(Query::new("SELECT 1 AS healthy")).await;
        expect_message!(client.read().await, RowDescription);
        assert_eq!(
            expect_message!(client.read().await, DataRow).get_int(0, true),
            Some(1)
        );
        expect_message!(client.read().await, CommandComplete);
        assert_eq!(
            expect_message!(client.read().await, ReadyForQuery).status,
            status
        );

        client.send_simple(Query::new("ROLLBACK")).await;
        client.read_until('Z').await.expect("end transaction");
    }
}

#[tokio::test]
async fn parser_off_rollback_recovers() {
    assert_recovery("ROLLBACK", 'I', false).await;
}

#[tokio::test]
async fn parser_off_rollback_to_savepoint_recovers() {
    assert_recovery("ROLLBACK TO SAVEPOINT s", 'T', false).await;
}

#[tokio::test]
async fn parser_off_extended_rollback_recovers() {
    assert_recovery("ROLLBACK", 'I', true).await;
}

#[tokio::test]
async fn parser_off_extended_rollback_to_savepoint_recovers() {
    assert_recovery("ROLLBACK TO SAVEPOINT s", 'T', true).await;
}

#[tokio::test]
async fn parser_off_transaction_end_aliases_recovers() {
    for sql in ["ABORT", "COMMIT", "END"] {
        assert_recovery(sql, 'I', false).await;
    }
}
