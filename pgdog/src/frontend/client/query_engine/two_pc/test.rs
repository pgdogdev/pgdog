use std::time::Duration;

use tokio::time::sleep;

use crate::{
    backend::{
        databases::databases,
        pool::{Connection, Request},
    },
    config,
    frontend::router::{parser::Shard, Route},
    net::Protocol,
};

use super::*;

#[tokio::test]
async fn test_cleanup_transaction_phase_one() {
    config::test::load_test();
    let cluster = databases().all().iter().next().unwrap().1.clone();

    let mut two_pc = TwoPc::default();
    let transaction = two_pc.transaction();

    let mut conn = Connection::new(cluster.user(), cluster.name(), false, &None).unwrap();
    conn.connect(&Request::default(), &Route::write(Shard::All))
        .await
        .unwrap();

    conn.execute("BEGIN").await.unwrap();
    let guard_1 = two_pc.phase_one(&cluster.identifier()).await.unwrap();
    let info = Manager::get().transaction(&transaction).unwrap();
    assert_eq!(info.phase, TwoPcPhase::Phase1);

    conn.two_pc(&transaction.to_string(), TwoPcPhase::Phase1)
        .await
        .unwrap();

    let two_pc = conn
        .execute("SELECT * FROM pg_prepared_xacts")
        .await
        .unwrap();
    // We have two-pc transactions.
    assert!(two_pc.iter().find(|p| p.code() == 'D').is_some());

    conn.disconnect();
    drop(guard_1);

    sleep(Duration::from_millis(100)).await;
    let transactions = Manager::get().transactions();
    assert!(transactions.is_empty());

    conn.connect(&Request::default(), &Route::write(Shard::All))
        .await
        .unwrap();

    let two_pc = conn
        .execute("SELECT * FROM pg_prepared_xacts")
        .await
        .unwrap();
    // No transactions.
    assert!(two_pc.iter().find(|p| p.code() == 'D').is_none());
}
