//! Integration tests for the RESET STATS admin command.
//!
//! One sequential test: RESET and RELOAD both act on global pool statistics,
//! so they must not run concurrently against the same pooler.

use crate::setup::*;
use rust_decimal::prelude::ToPrimitive;
use sqlx::{Executor, Pool, Postgres, Row};

/// Sum of `total_query_count` across every pool reported by SHOW STATS.
///
/// The column is sent as NUMERIC, so it's decoded via Decimal.
async fn total_query_count(admin: &Pool<Postgres>) -> i64 {
    admin
        .fetch_all("SHOW STATS")
        .await
        .unwrap()
        .iter()
        .map(|row| {
            row.get::<rust_decimal::Decimal, _>("total_query_count")
                .to_i64()
                .unwrap()
        })
        .sum()
}

#[tokio::test]
async fn test_reload_preserves_and_reset_clears_pool_stats() {
    let admin = admin_sqlx().await;

    // Bump pool statistics by running queries through the pooler.
    let client = connections_sqlx().await.pop().unwrap();
    for _ in 0..20 {
        let _: (i64,) = sqlx::query_as("SELECT 1::BIGINT")
            .fetch_one(&client)
            .await
            .unwrap();
    }

    let before = total_query_count(&admin).await;
    assert!(before >= 20, "expected client queries to be counted");

    // RELOAD must not lose the accumulated counters (issue #1281).
    admin.execute("RELOAD").await.unwrap();
    let after_reload = total_query_count(&admin).await;
    assert!(
        after_reload >= before,
        "RELOAD must preserve pool statistics (before {before}, after {after_reload})"
    );

    // RESET STATS must zero them.
    admin.execute("RESET STATS").await.unwrap();
    let after_reset = total_query_count(&admin).await;
    assert_eq!(
        after_reset, 0,
        "RESET STATS must zero pool query counters (got {after_reset})"
    );

    // Counters keep counting from zero afterwards.
    let _: (i64,) = sqlx::query_as("SELECT 1::BIGINT")
        .fetch_one(&client)
        .await
        .unwrap();
    let resumed = total_query_count(&admin).await;
    assert!(
        resumed > 0,
        "statistics must resume counting after RESET STATS"
    );
}
