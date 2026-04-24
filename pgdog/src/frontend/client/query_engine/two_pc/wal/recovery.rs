//! Two-phase commit WAL recovery.
//!
//! Replays every record in every existing segment under the WAL directory
//! to rebuild the set of in-flight 2PC transactions, then hands each one
//! back to [`Manager`] via [`Manager::restore_transaction`] so the monitor
//! task drives them to a terminal state via the existing
//! `cleanup_phase` machinery.
//!
//! Returns the writable [`Segment`] the writer task should continue
//! appending to. If the directory had no segments, a fresh one is created
//! at LSN 0.

use std::path::Path;

use fnv::FnvHashMap as HashMap;

use super::error::Error;
use super::record::{CheckpointEntry, Record};
use super::segment::{list_segments, Segment, SegmentReader};
use crate::frontend::client::query_engine::two_pc::{Manager, TwoPcPhase, TwoPcTransaction};

/// Working entry held only for the duration of [`recover_transactions`].
/// Each surviving entry becomes a [`Manager::restore_transaction`] call.
struct Entry {
    user: String,
    database: String,
    decided: bool,
}

/// Scan every segment in `dir` in LSN order, hand each in-flight
/// transaction to `manager`, and return the [`Segment`] for the writer
/// task to continue appending to.
pub(super) async fn recover_transactions(manager: &Manager, dir: &Path) -> Result<Segment, Error> {
    let segments = list_segments(dir).await?;
    let mut working: HashMap<TwoPcTransaction, Entry> = HashMap::default();

    let Some((last_path, prior_paths)) = segments.split_last() else {
        return Segment::create(dir, 0).await;
    };

    for path in prior_paths {
        let mut reader = SegmentReader::open(path).await?;
        while let Some(record) = reader.next().await? {
            apply(&mut working, record);
        }
    }

    let mut last_reader = SegmentReader::open(last_path).await?;
    while let Some(record) = last_reader.next().await? {
        apply(&mut working, record);
    }

    // Restore before truncating the last segment: if `into_writable`
    // fails we still surface the recovered txns to the manager so they
    // can be driven to a terminal state on the backends.
    let restored = working.len();
    for (txn, entry) in working {
        let phase = if entry.decided {
            TwoPcPhase::Phase2
        } else {
            TwoPcPhase::Phase1
        };
        manager.restore_transaction(txn, entry.user, entry.database, phase);
    }
    if restored > 0 {
        tracing::info!(
            "2pc wal: restored {} in-flight transaction(s) from {} segment(s)",
            restored,
            segments.len()
        );
    }

    last_reader.into_writable().await
}

fn apply(working: &mut HashMap<TwoPcTransaction, Entry>, record: Record) {
    match record {
        Record::Begin(p) => {
            working.insert(
                p.txn,
                Entry {
                    user: p.user,
                    database: p.database,
                    decided: false,
                },
            );
        }
        Record::Committing(p) => {
            if let Some(entry) = working.get_mut(&p.txn) {
                entry.decided = true;
            }
        }
        Record::End(p) => {
            working.remove(&p.txn);
        }
        Record::Checkpoint(p) => {
            working.clear();
            for CheckpointEntry {
                txn,
                user,
                database,
                decided,
                ..
            } in p.active
            {
                working.insert(
                    txn,
                    Entry {
                        user,
                        database,
                        decided,
                    },
                );
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::super::record::{BeginPayload, CheckpointPayload, TxnPayload};
    use super::*;

    fn txn(n: usize) -> TwoPcTransaction {
        // Inner field is private; round-trip via Display/FromStr so tests
        // can build deterministic ids.
        format!("__pgdog_2pc_{}", n).parse().unwrap()
    }

    #[test]
    fn begin_inserts_undecided() {
        let mut w: HashMap<TwoPcTransaction, Entry> = HashMap::default();
        apply(
            &mut w,
            Record::Begin(BeginPayload {
                txn: txn(1),
                user: "alice".into(),
                database: "shop".into(),
                shards: vec![0, 1],
            }),
        );
        let entry = w.get(&txn(1)).unwrap();
        assert_eq!(entry.user, "alice");
        assert_eq!(entry.database, "shop");
        assert!(!entry.decided);
    }

    #[test]
    fn committing_marks_decided() {
        let mut w: HashMap<TwoPcTransaction, Entry> = HashMap::default();
        apply(
            &mut w,
            Record::Begin(BeginPayload {
                txn: txn(1),
                user: "u".into(),
                database: "d".into(),
                shards: vec![0],
            }),
        );
        apply(&mut w, Record::Committing(TxnPayload { txn: txn(1) }));
        assert!(w.get(&txn(1)).unwrap().decided);
    }

    #[test]
    fn committing_without_begin_is_ignored() {
        let mut w: HashMap<TwoPcTransaction, Entry> = HashMap::default();
        apply(&mut w, Record::Committing(TxnPayload { txn: txn(1) }));
        assert!(w.is_empty());
    }

    #[test]
    fn end_removes() {
        let mut w: HashMap<TwoPcTransaction, Entry> = HashMap::default();
        apply(
            &mut w,
            Record::Begin(BeginPayload {
                txn: txn(1),
                user: "u".into(),
                database: "d".into(),
                shards: vec![0],
            }),
        );
        apply(&mut w, Record::End(TxnPayload { txn: txn(1) }));
        assert!(w.is_empty());
    }

    #[test]
    fn checkpoint_replaces_state() {
        let mut w: HashMap<TwoPcTransaction, Entry> = HashMap::default();
        apply(
            &mut w,
            Record::Begin(BeginPayload {
                txn: txn(1),
                user: "u1".into(),
                database: "d1".into(),
                shards: vec![0],
            }),
        );
        apply(
            &mut w,
            Record::Begin(BeginPayload {
                txn: txn(2),
                user: "u2".into(),
                database: "d2".into(),
                shards: vec![0],
            }),
        );
        apply(
            &mut w,
            Record::Checkpoint(CheckpointPayload {
                active: vec![CheckpointEntry {
                    txn: txn(99),
                    user: "u99".into(),
                    database: "d99".into(),
                    shards: vec![0],
                    decided: true,
                }],
            }),
        );
        assert_eq!(w.len(), 1);
        let entry = w.get(&txn(99)).unwrap();
        assert_eq!(entry.user, "u99");
        assert!(entry.decided);
    }
}
