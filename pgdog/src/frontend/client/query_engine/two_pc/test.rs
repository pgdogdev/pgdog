use crate::{
    backend::{
        Server,
        databases::databases,
        pool::{Connection, Request},
    },
    config,
    frontend::router::{
        Route,
        parser::{Shard, ShardWithPriority},
    },
    logger,
    net::Protocol,
};

use super::*;
use super::{server_transactions::TwoPcServerTransaction, statement::phase_control};

async fn server_has_transaction(server: &mut Server, transaction: TwoPcTransaction) -> bool {
    TwoPcTransactions::load(server)
        .await
        .unwrap()
        .iter()
        .any(|server_transaction| {
            matches!(
                server_transaction,
                TwoPcServerTransaction::Ours { txn, .. } if *txn == transaction
            )
        })
}

#[tokio::test]
async fn test_cleanup_abandoned() {
    config::load_test_with_user("pgdog");
    let cluster = databases().all().iter().next().unwrap().1.clone();
    let transaction = TwoPcTransaction::new();
    let mut conn = cluster.shards()[0]
        .primary(&Request::default())
        .await
        .unwrap();

    conn.execute("BEGIN").await.unwrap();
    conn.execute(phase_control(transaction, 0, TwoPcPhase::Phase1))
        .await
        .unwrap();

    assert!(server_has_transaction(&mut conn, transaction).await);

    Manager::get().cleanup_abandoned().await.unwrap();

    assert!(!server_has_transaction(&mut conn, transaction).await);
}

#[tokio::test]
async fn test_cleanup_abandoned_different_user() {
    config::load_test_with_user("pgdog1");
    let cluster = databases().all().iter().next().unwrap().1.clone();
    let transaction = TwoPcTransaction::new();
    let mut conn = cluster.shards()[0]
        .primary(&Request::default())
        .await
        .unwrap();

    conn.execute("BEGIN").await.unwrap();
    conn.execute(phase_control(transaction, 0, TwoPcPhase::Phase1))
        .await
        .unwrap();
    assert!(server_has_transaction(&mut conn, transaction).await);
    drop(conn);

    config::load_test_with_user("pgdog2");
    Manager::get().cleanup_abandoned().await.unwrap();

    config::load_test_with_user("pgdog1");
    let cluster = databases().all().iter().next().unwrap().1.clone();
    let mut conn = cluster.shards()[0]
        .primary(&Request::default())
        .await
        .unwrap();
    let was_preserved = server_has_transaction(&mut conn, transaction).await;
    conn.execute(phase_control(transaction, 0, TwoPcPhase::Rollback))
        .await
        .unwrap();

    assert!(was_preserved);
}

#[tokio::test]
async fn test_cleanup_transaction_phase_one() {
    config::load_test();
    let cluster = databases().all().iter().next().unwrap().1.clone();

    let mut two_pc = TwoPc::default();
    let transaction = two_pc.transaction();

    let mut conn = Connection::new(cluster.user(), cluster.name(), false).unwrap();
    conn.connect(
        &Request::default(),
        &Route::write(ShardWithPriority::new_default_unset(Shard::All)),
    )
    .await
    .unwrap();

    conn.execute("BEGIN").await.unwrap();
    conn.execute("CREATE TABLE test_cleanup_transaction_phase_one(id BIGINT)")
        .await
        .unwrap();
    let guard_1 = two_pc.phase_one(&cluster.identifier()).await.unwrap();
    let info = Manager::get().transaction(&transaction).unwrap();
    assert_eq!(info.phase, TwoPcPhase::Phase1);

    conn.two_pc(transaction, TwoPcPhase::Phase1).await.unwrap();

    let two_pc = conn
        .execute("SELECT * FROM pg_prepared_xacts")
        .await
        .unwrap();
    // We have two-pc transactions.
    assert!(two_pc.iter().find(|p| p.code() == 'D').is_some());

    // Simulate client disconnecting abruptly.
    conn.disconnect();
    drop(guard_1);

    // Shutdown manager cleanly.
    Manager::get().shutdown().await;

    let transactions = Manager::get().transactions();
    assert!(transactions.is_empty());

    conn.connect(
        &Request::default(),
        &Route::write(ShardWithPriority::new_default_unset(Shard::All)),
    )
    .await
    .unwrap();

    let two_pc = conn
        .execute("SELECT * FROM pg_prepared_xacts")
        .await
        .unwrap();
    // No transactions.
    assert!(two_pc.iter().find(|p| p.code() == 'D').is_none());
    // Table wasn't committed.
    let table = conn
        .execute("SELECT * FROM test_cleanup_transaction_phase_one")
        .await
        .err()
        .unwrap();
    assert!(
        table
            .to_string()
            .contains(r#"relation "test_cleanup_transaction_phase_one" does not exist"#)
    );
}

#[tokio::test]
async fn test_cleanup_transaction_phase_two() {
    config::load_test();
    logger();
    let cluster = databases().all().iter().next().unwrap().1.clone();

    let mut two_pc = TwoPc::default();
    let transaction = two_pc.transaction();

    let mut conn = Connection::new(cluster.user(), cluster.name(), false).unwrap();
    conn.connect(
        &Request::default(),
        &Route::write(ShardWithPriority::new_default_unset(Shard::All)),
    )
    .await
    .unwrap();

    conn.execute("BEGIN").await.unwrap();
    conn.execute("CREATE TABLE test_cleanup_transaction_phase_two(id BIGINT)")
        .await
        .unwrap();
    let guard_1 = two_pc.phase_one(&cluster.identifier()).await.unwrap();
    let info = Manager::get().transaction(&transaction).unwrap();
    assert_eq!(info.phase, TwoPcPhase::Phase1);

    conn.two_pc(transaction, TwoPcPhase::Phase1).await.unwrap();

    let txns = conn
        .execute("SELECT * FROM pg_prepared_xacts")
        .await
        .unwrap();
    // We have two-pc transactions.
    assert!(txns.iter().find(|p| p.code() == 'D').is_some());

    let guard_2 = two_pc.phase_two(&cluster.identifier()).await.unwrap();
    let info = Manager::get().transaction(&transaction).unwrap();
    assert_eq!(info.phase, TwoPcPhase::Phase2);

    // Simulate client disconnecting abruptly.
    conn.disconnect();
    drop(guard_1);
    drop(guard_2);

    // Shutdown manager cleanly.
    Manager::get().shutdown().await;

    let transactions = Manager::get().transactions();
    assert!(transactions.is_empty());

    conn.connect(
        &Request::default(),
        &Route::write(ShardWithPriority::new_default_unset(Shard::All)),
    )
    .await
    .unwrap();

    let two_pc = conn
        .execute("SELECT * FROM pg_prepared_xacts")
        .await
        .unwrap();
    // No transactions.
    assert!(two_pc.iter().find(|p| p.code() == 'D').is_none());
    // Table was committed.
    let _table = conn
        .execute("SELECT * FROM test_cleanup_transaction_phase_two")
        .await
        .unwrap();
    conn.execute("DROP TABLE test_cleanup_transaction_phase_two")
        .await
        .unwrap();
}
