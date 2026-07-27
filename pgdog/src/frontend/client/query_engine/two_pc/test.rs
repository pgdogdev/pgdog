use crate::{
    backend::{
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
async fn test_cleanup_transaction_foreign_prefix() {
    config::load_test();
    logger();
    let cluster = databases().all().iter().next().unwrap().1.clone();

    // A transaction restored from the WAL after a restart. The GIDs on
    // the shards carry the previous process's prefix, known here from
    // the recorded WAL Begin record. The numeric ID is random so a
    // failed run's leftover prepared transaction can't block reruns.
    let number: u64 = rand::random();
    let transaction: TwoPcTransaction = format!("__pgdog_2pc_{number}").parse().unwrap();
    let foreign_gid = format!("__pgdog_2pc_previousinstance_{number}_0");
    let table = format!("test_cleanup_foreign_prefix_{number}");

    let mut conn = Connection::new(cluster.user(), cluster.name(), false).unwrap();
    conn.connect(
        &Request::default(),
        &Route::write(ShardWithPriority::new_default_unset(Shard::All)),
    )
    .await
    .unwrap();
    conn.execute("BEGIN").await.unwrap();
    conn.execute(format!("CREATE TABLE {table}(id BIGINT)"))
        .await
        .unwrap();
    conn.execute(format!("PREPARE TRANSACTION '{foreign_gid}'"))
        .await
        .unwrap();
    conn.disconnect();

    let manager = Manager::get();
    manager.restore_transaction(
        transaction,
        cluster.user().to_string(),
        cluster.name().to_string(),
        "__pgdog_2pc_previousinstance_".to_string(),
        TwoPcPhase::Phase1,
    );

    manager.wait_until_cleaned_up(transaction).await;
    assert!(manager.transaction(&transaction).is_none());

    conn.connect(
        &Request::default(),
        &Route::write(ShardWithPriority::new_default_unset(Shard::All)),
    )
    .await
    .unwrap();

    let leftover = conn
        .execute(format!(
            "SELECT gid FROM pg_prepared_xacts WHERE gid = '{foreign_gid}'"
        ))
        .await
        .unwrap();
    assert!(
        leftover.iter().find(|p| p.code() == 'D').is_none(),
        "prepared transaction with the previous process's prefix was not cleaned up"
    );

    // Phase 1 transactions are rolled back: the table doesn't exist.
    let missing = conn
        .execute(format!("SELECT * FROM {table}"))
        .await
        .err()
        .unwrap();
    assert!(missing.to_string().contains("does not exist"));
}

#[tokio::test]
async fn test_cleanup_transaction_legacy_record() {
    config::load_test();
    logger();
    let cluster = databases().all().iter().next().unwrap().1.clone();

    // A transaction restored from a WAL record written by a version
    // that did not store the coordinator GID prefix: cleanup falls
    // back to matching by the durable numeric ID.
    let number: u64 = rand::random();
    let transaction: TwoPcTransaction = format!("__pgdog_2pc_{number}").parse().unwrap();
    let foreign_gid = format!("__pgdog_2pc_legacyinstance_{number}_0");

    let mut conn = Connection::new(cluster.user(), cluster.name(), false).unwrap();
    conn.connect(
        &Request::default(),
        &Route::write(ShardWithPriority::new_default_unset(Shard::All)),
    )
    .await
    .unwrap();
    conn.execute("BEGIN").await.unwrap();
    conn.execute("SELECT 1").await.unwrap();
    conn.execute(format!("PREPARE TRANSACTION '{foreign_gid}'"))
        .await
        .unwrap();
    conn.disconnect();

    let manager = Manager::get();
    manager.restore_transaction(
        transaction,
        cluster.user().to_string(),
        cluster.name().to_string(),
        String::new(),
        TwoPcPhase::Phase1,
    );

    manager.wait_until_cleaned_up(transaction).await;
    assert!(manager.transaction(&transaction).is_none());

    conn.connect(
        &Request::default(),
        &Route::write(ShardWithPriority::new_default_unset(Shard::All)),
    )
    .await
    .unwrap();

    let leftover = conn
        .execute(format!(
            "SELECT gid FROM pg_prepared_xacts WHERE gid = '{foreign_gid}'"
        ))
        .await
        .unwrap();
    assert!(
        leftover.iter().find(|p| p.code() == 'D').is_none(),
        "prepared transaction from a legacy WAL record was not cleaned up"
    );
}

#[tokio::test]
async fn test_cleanup_leaves_other_prefixes_alone() {
    config::load_test();
    logger();
    let cluster = databases().all().iter().next().unwrap().1.clone();

    // A prepared transaction that shares the numeric ID but carries a
    // different coordinator prefix belongs to someone else: cleanup
    // with a recorded prefix must not touch it.
    let number: u64 = rand::random();
    let transaction: TwoPcTransaction = format!("__pgdog_2pc_{number}").parse().unwrap();
    let other_gid = format!("__pgdog_2pc_otherinstance_{number}_0");

    let mut conn = Connection::new(cluster.user(), cluster.name(), false).unwrap();
    conn.connect(
        &Request::default(),
        &Route::write(ShardWithPriority::new_default_unset(Shard::All)),
    )
    .await
    .unwrap();
    conn.execute("BEGIN").await.unwrap();
    conn.execute("SELECT 1").await.unwrap();
    conn.execute(format!("PREPARE TRANSACTION '{other_gid}'"))
        .await
        .unwrap();
    conn.disconnect();

    let manager = Manager::get();
    manager.restore_transaction(
        transaction,
        cluster.user().to_string(),
        cluster.name().to_string(),
        "__pgdog_2pc_recordedinstance_".to_string(),
        TwoPcPhase::Phase1,
    );

    manager.wait_until_cleaned_up(transaction).await;
    // The recorded GID exists nowhere, so the transaction resolves.
    assert!(manager.transaction(&transaction).is_none());

    conn.connect(
        &Request::default(),
        &Route::write(ShardWithPriority::new_default_unset(Shard::All)),
    )
    .await
    .unwrap();

    let survivor = conn
        .execute(format!(
            "SELECT gid FROM pg_prepared_xacts WHERE gid = '{other_gid}'"
        ))
        .await
        .unwrap();
    assert!(
        survivor.iter().find(|p| p.code() == 'D').is_some(),
        "prepared transaction under a different prefix was resolved by cleanup"
    );

    conn.execute(format!("ROLLBACK PREPARED '{other_gid}'"))
        .await
        .unwrap();
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
