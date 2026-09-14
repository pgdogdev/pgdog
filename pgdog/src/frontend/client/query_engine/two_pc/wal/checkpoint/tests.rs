use std::sync::Arc;

use bytes::{BufMut, BytesMut};
use fnv::FnvHashSet as HashSet;
use tempfile::TempDir;

use super::*;
use crate::{backend::databases::User, frontend::client::query_engine::TwoPcPhase, net::ToBytes};

fn identifier() -> Arc<User> {
    Arc::new(User {
        user: "pgdog".into(),
        database: "pgdog".into(),
    })
}

fn identity(transaction: TwoPcTransaction) -> Record {
    TwoPcRecordIdentity {
        transaction,
        identifier: identifier(),
    }
    .into()
}

async fn write_segment(tmp: &TempDir, id: u64, records: Vec<Record>) {
    let mut bytes = BytesMut::new();
    bytes.put_u64(id);
    bytes.put_u32(0);
    for record in records {
        bytes.extend_from_slice(&record.to_bytes());
    }
    tokio::fs::write(Segment::path(tmp.path(), id), bytes)
        .await
        .expect("write WAL segment");
    SegmentRegistry::get().record(id, SegmentStatus::Inactive);
}

async fn replay(tmp: &TempDir) -> Manager {
    let manager = Manager::init();
    let mut identities = HashSet::default();
    let recovery = Recovery::new(&tmp.path().to_owned())
        .await
        .expect("list WAL");
    for (_, path) in recovery.files {
        let segment = Segment::load(&path).await.expect("load WAL");
        // Check dependencies explicitly, independently of how recovery handles
        // orphan phase records left by older versions of the checkpointer.
        for record in &segment.records {
            match Records::try_from(record.clone()).expect("valid WAL record") {
                Records::Identity(record) => {
                    identities.insert(record.transaction);
                }
                Records::Phase(record) => assert!(identities.contains(&record.transaction)),
                Records::Remove(record) => {
                    identities.remove(&record.transaction);
                }
            }
        }
        segment.replay(&manager).expect("replay WAL");
    }
    manager
}

#[tokio::test]
async fn test_checkpoint_retains_identity_segment() {
    let tmp = TempDir::new().expect("temporary WAL directory");
    let manager = Manager::init();
    let completed = TwoPcTransaction::new();
    let preparing = TwoPcTransaction::new();
    let committing = TwoPcTransaction::new();

    write_segment(&tmp, 1, vec![identity(completed)]).await;
    write_segment(
        &tmp,
        2,
        vec![
            TwoPcRecordPhase::new(completed).into(),
            identity(preparing),
            identity(committing),
            TwoPcRecordPhase::new(committing).into(),
        ],
    )
    .await;

    manager.set_transaction_state(preparing, &identifier(), TwoPcPhase::Phase1);
    manager.set_transaction_state(committing, &identifier(), TwoPcPhase::Phase2);
    Checkpointer::new(tmp.path(), manager, Duration::from_secs(1))
        .run_once()
        .await
        .expect("checkpoint completed transactions");

    assert!(Segment::path(tmp.path(), 1).exists());
    assert!(Segment::path(tmp.path(), 2).exists());
    let recovered = replay(&tmp).await;
    for (transaction, phase) in [
        (completed, TwoPcPhase::Phase2),
        (preparing, TwoPcPhase::Phase1),
        (committing, TwoPcPhase::Phase2),
    ] {
        let info = recovered
            .transaction(&transaction)
            .expect("transaction recovered");
        assert_eq!(info.phase, phase);
        assert_eq!(info.identifier, identifier());
    }
}

#[tokio::test]
async fn test_checkpoint_retains_transitive_dependencies() {
    let tmp = TempDir::new().expect("temporary WAL directory");
    let manager = Manager::init();
    let a = TwoPcTransaction::new();
    let b = TwoPcTransaction::new();
    let active = TwoPcTransaction::new();
    write_segment(&tmp, 1, vec![identity(a)]).await;
    write_segment(&tmp, 2, vec![TwoPcRecordPhase::new(a).into(), identity(b)]).await;
    write_segment(
        &tmp,
        3,
        vec![TwoPcRecordPhase::new(b).into(), identity(active)],
    )
    .await;
    write_segment(&tmp, 4, vec![identity(TwoPcTransaction::new())]).await;

    manager.set_transaction_state(active, &identifier(), TwoPcPhase::Phase1);
    let checkpointer = Checkpointer::new(tmp.path(), manager.clone(), Duration::from_secs(1));
    checkpointer.run_once().await.expect("checkpoint");
    for id in 1..=3 {
        assert!(Segment::path(tmp.path(), id).exists());
    }
    assert!(!Segment::path(tmp.path(), 4).exists());
    replay(&tmp).await;

    manager
        .done(active)
        .await
        .expect("finish active transaction");
    checkpointer
        .run_once()
        .await
        .expect("checkpoint after completion");
    for id in 1..=4 {
        assert!(!Segment::path(tmp.path(), id).exists());
    }
}

#[tokio::test(start_paused = true)]
async fn test_checkpoint_retains_identity_for_queued_phase() {
    let tmp = TempDir::new().expect("temporary WAL directory");
    let transaction = TwoPcTransaction::new();
    write_segment(&tmp, 1, vec![identity(transaction)]).await;
    let live = LiveSegment::new(tmp.path(), 2, Duration::from_secs(60))
        .await
        .expect("live segment");
    SegmentRegistry::get().record(2, SegmentStatus::Active);
    let waiter = live.add(TwoPcRecordPhase::new(transaction).into());

    // Deliberately omit the transaction from the manager: the dependency must
    // protect its identity even after the transaction has completed.
    let checkpointer = Checkpointer::new(tmp.path(), Manager::init(), Duration::from_secs(1));
    checkpointer
        .run_once()
        .await
        .expect("checkpoint queued phase");
    assert!(waiter.waiter.is_empty(), "phase should still be queued");
    assert!(Segment::path(tmp.path(), 1).exists());

    live.shutdown();
    waiter.wait_flush().await;
    replay(&tmp).await;
    checkpointer
        .run_once()
        .await
        .expect("checkpoint flushed phase");
    assert!(!Segment::path(tmp.path(), 1).exists());
    assert!(!Segment::path(tmp.path(), 2).exists());
}

#[tokio::test]
async fn test_checkpoint_deletion_boundaries_preserve_replay() {
    let tmp = TempDir::new().expect("temporary WAL directory");
    let transaction = TwoPcTransaction::new();
    write_segment(&tmp, 1, vec![identity(transaction)]).await;
    write_segment(&tmp, 2, vec![TwoPcRecordPhase::new(transaction).into()]).await;
    write_segment(&tmp, 3, vec![TwoPcRecordRemove { transaction }.into()]).await;
    let manager = Manager::init();
    let checkpointer = Checkpointer::new(tmp.path(), manager.clone(), Duration::from_secs(1));
    let mut segments = Vec::new();
    for id in 1..=3 {
        let segment = Segment::load(&Segment::path(tmp.path(), id))
            .await
            .expect("load segment");
        segments.push(SegmentDependencies::new(segment).expect("segment dependencies"));
    }
    let candidates = selection::candidates(&segments, &manager);
    assert_eq!(candidates, vec![3, 2, 1]);
    for id in candidates {
        checkpointer
            .remove_segment(id)
            .await
            .expect("durable deletion");
        replay(&tmp).await;
    }
}

#[tokio::test]
async fn test_checkpoint_resumes_after_unlink() {
    let tmp = TempDir::new().expect("temporary WAL directory");
    let transaction = TwoPcTransaction::new();
    write_segment(&tmp, 1, vec![identity(transaction)]).await;
    write_segment(&tmp, 2, vec![TwoPcRecordPhase::new(transaction).into()]).await;
    // Simulate interruption between unlink and registry removal.
    tokio::fs::remove_file(Segment::path(tmp.path(), 2))
        .await
        .expect("unlink phase segment");
    Checkpointer::new(tmp.path(), Manager::init(), Duration::from_secs(1))
        .run_once()
        .await
        .expect("resume checkpoint");
    assert!(!Segment::path(tmp.path(), 1).exists());
    assert_eq!(SegmentRegistry::get().len(), 0);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_checkpoint_during_concurrent_phase_writes() {
    let tmp = Arc::new(TempDir::new().expect("temporary WAL directory"));
    let manager = Manager::init();
    let checkpointer = Checkpointer::new(tmp.path(), manager.clone(), Duration::from_secs(1));
    let stop = CancellationToken::new();
    let background = {
        let checkpointer = checkpointer.clone();
        let stop = stop.clone();
        tokio::spawn(async move {
            while !stop.is_cancelled() {
                checkpointer
                    .run_once()
                    .await
                    .expect("concurrent checkpoint");
                tokio::task::yield_now().await;
            }
        })
    };

    let mut clients = Vec::new();
    for client in 0..16 {
        let tmp = tmp.clone();
        let manager = manager.clone();
        clients.push(tokio::spawn(async move {
            let transaction = TwoPcTransaction::new();
            for (offset, phase, record) in [
                (1, TwoPcPhase::Phase1, identity(transaction)),
                (
                    2,
                    TwoPcPhase::Phase2,
                    TwoPcRecordPhase::new(transaction).into(),
                ),
            ] {
                manager.set_transaction_state(transaction, &identifier(), phase);
                let segment = LiveSegment::new(tmp.path(), client * 2 + offset, Duration::ZERO)
                    .await
                    .expect("new segment");
                let waiter = segment.add(record);
                segment.shutdown();
                waiter.wait_flush().await;
            }
            transaction
        }));
    }
    let mut transactions = Vec::new();
    for client in clients {
        transactions.push(client.await.expect("client phase writes"));
    }
    stop.cancel();
    background.await.expect("checkpoint task");

    // All transactions are still active, with their identities and phases in
    // separate closed segments despite concurrent checkpoint selection.
    let recovered = replay(&tmp).await;
    for transaction in transactions {
        assert_eq!(
            recovered
                .transaction(&transaction)
                .expect("active transaction retained")
                .phase,
            TwoPcPhase::Phase2
        );
        manager.done(transaction).await.expect("finish transaction");
    }
    checkpointer
        .run_once()
        .await
        .expect("checkpoint completed workload");
    assert!(replay(&tmp).await.transactions().is_empty());
}
