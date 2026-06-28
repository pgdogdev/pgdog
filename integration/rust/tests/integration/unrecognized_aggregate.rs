use crate::setup::{admin_sqlx, connections_sqlx};
use sqlx::Executor;
use std::assert_matches;

async fn define_custom_aggregate_fn() {
    for connection in connections_sqlx().await {
        connection
            .execute("DROP AGGREGATE IF EXISTS pgdog_sum (int4)")
            .await
            .unwrap();
        connection
            .execute("CREATE AGGREGATE pgdog_sum (int4) (sfunc = int4_sum, stype = bigint)")
            .await
            .unwrap();
        connection
            .execute("DROP TABLE IF EXISTS unrecognized_agg_test")
            .await
            .unwrap();
        connection
            .execute("CREATE TABLE unrecognized_agg_test (lol int4, customer_id bigint)")
            .await
            .unwrap();
    }
    admin_sqlx().await.execute("RELOAD").await.unwrap();
}

#[tokio::test]
async fn unrecognized_aggregate_function_errors_only_on_cross_shard_queries() {
    define_custom_aggregate_fn().await;
    let mut connections = connections_sqlx().await;
    let sharded = connections.pop().unwrap();
    let unsharded = connections.pop().unwrap();

    let unsharded_query = unsharded
        .fetch_one("SELECT pgdog_sum(lol) FROM unrecognized_agg_test")
        .await;
    assert_matches!(unsharded_query, Ok(_));

    let sharded_query = sharded
        .fetch_one("SELECT pgdog_sum(lol) FROM unrecognized_agg_test")
        .await;
    let err = sharded_query.expect_err("unrecognized aggregate executed successfully");
    assert!(err.to_string().contains("pgdog_sum() is not yet supported"));
}
