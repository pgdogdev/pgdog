use anyhow::{Context, Result, ensure};
use serial_test::serial;
use tokio_postgres::{SimpleQueryMessage, error::SqlState};

use crate::setup::{admin_tokio, connections_tokio};

#[tokio::test]
#[serial]
async fn empty_query_then_rollback_parser_on() -> Result<()> {
    check_empty_query_then_rollback("on").await
}

#[tokio::test]
#[serial]
async fn empty_query_then_rollback_parser_auto() -> Result<()> {
    check_empty_query_then_rollback("auto").await
}

#[tokio::test]
#[serial]
async fn empty_query_then_rollback_parser_off() -> Result<()> {
    check_empty_query_then_rollback("off").await
}

async fn check_empty_query_then_rollback(level: &str) -> Result<()> {
    let admin = admin_tokio().await;
    admin
        .batch_execute(&format!("SET query_parser TO '{level}'"))
        .await?;

    let result = async {
        let clients = connections_tokio().await;
        let client = &clients[0];
        client.batch_execute("BEGIN").await?;
        let error = client
            .simple_query("SELECT 1/0")
            .await
            .err()
            .context("division by zero must fail")?;
        ensure!(error.code() == Some(&SqlState::DIVISION_BY_ZERO));

        let messages = client.simple_query(";").await?;
        // tokio-postgres exposes EmptyQueryResponse as CommandComplete(0).
        ensure!(matches!(
            messages.as_slice(),
            [SimpleQueryMessage::CommandComplete(0)]
        ));

        // An empty query must not clear the failed transaction state.
        let error = client
            .simple_query("SELECT 1")
            .await
            .err()
            .context("query in failed transaction must fail")?;
        ensure!(error.code() == Some(&SqlState::IN_FAILED_SQL_TRANSACTION));

        client.batch_execute("ROLLBACK").await?;
        client.simple_query("SELECT 1").await?;
        Ok(())
    }
    .await;

    admin.batch_execute("RELOAD").await?;
    result
}
