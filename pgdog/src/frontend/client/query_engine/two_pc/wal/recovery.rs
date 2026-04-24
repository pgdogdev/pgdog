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
///
/// Corrupt segments are renamed to `<lsn>.wal.broken` and skipped. If
/// any corruption is detected the restore phase is skipped — partial
/// restore could silently invert a committed transaction by losing a
/// `Committing` record. The operator handles orphan prepared xacts via
/// `SHOW TRANSACTIONS` / `pg_prepared_xacts`. Genuine IO errors
/// propagate; the caller treats those as "WAL not usable."
pub(super) async fn recover_transactions(manager: &Manager, dir: &Path) -> Result<Segment, Error> {
    let segments = list_segments(dir).await?;
    let mut working: HashMap<TwoPcTransaction, Entry> = HashMap::default();
    let mut corruption = false;
    let mut next_lsn: u64 = 0;

    let Some((last_path, prior_paths)) = segments.split_last() else {
        return Segment::create(dir, 0).await;
    };

    for path in prior_paths {
        let mut reader = match SegmentReader::open(path).await {
            Ok(reader) => reader,
            Err(err) if err.is_corruption() => {
                quarantine(path, &err).await;
                corruption = true;
                continue;
            }
            Err(err) => return Err(err),
        };
        let drained = drain(&mut reader, &mut working).await;
        next_lsn = reader.next_lsn();
        match drained {
            Ok(()) => {}
            Err(err) if err.is_corruption() => {
                drop(reader);
                quarantine(path, &err).await;
                corruption = true;
            }
            Err(err) => return Err(err),
        }
    }

    let last_writable = match SegmentReader::open(last_path).await {
        Ok(mut reader) => {
            let drained = drain(&mut reader, &mut working).await;
            next_lsn = reader.next_lsn();
            match drained {
                Ok(()) | Err(Error::TornTail { .. }) => Some(reader.into_writable().await?),
                Err(err) if err.is_corruption() => {
                    drop(reader);
                    quarantine(last_path, &err).await;
                    corruption = true;
                    None
                }
                Err(err) => return Err(err),
            }
        }
        Err(err) if err.is_corruption() => {
            quarantine(last_path, &err).await;
            corruption = true;
            None
        }
        Err(err) => return Err(err),
    };

    // Decided txns (Committing was logged) are safe to restore even if
    // corruption was detected later: COMMIT PREPARED is idempotent and
    // matches the decision we durably recorded. Undecided txns can only
    // be restored when there was no corruption — otherwise their
    // Committing might have been in a lost segment and rolling back
    // would silently invert a committed transaction.
    for (txn, entry) in working {
        let phase = match (entry.decided, corruption) {
            (true, _) => TwoPcPhase::Phase2,
            (false, false) => TwoPcPhase::Phase1,
            (false, true) => continue,
        };
        manager.restore_transaction(txn, entry.user, entry.database, phase);
    }
    if corruption {
        tracing::warn!(
            "[2pc] wal recovery detected corruption; undecided transactions \
             must be reconciled via SHOW TRANSACTIONS / pg_prepared_xacts"
        );
    }

    match last_writable {
        Some(segment) => Ok(segment),
        None => Segment::create(dir, next_lsn).await,
    }
}

/// Drain every record in `reader` into `working`. Errors propagate as
/// usual; the caller decides what to do based on the error variant.
async fn drain(
    reader: &mut SegmentReader,
    working: &mut HashMap<TwoPcTransaction, Entry>,
) -> Result<(), Error> {
    while let Some(record) = reader.next().await? {
        apply(working, record);
    }
    Ok(())
}

/// Log corruption and rename the segment to `<original>.broken` so
/// subsequent recoveries skip it. Best-effort: rename failures are
/// logged.
async fn quarantine(path: &Path, err: &Error) {
    tracing::warn!("[2pc] corrupt wal segment {}: {}", path.display(), err);
    let broken = path.with_extension("wal.broken");
    if let Err(rename_err) = tokio::fs::rename(path, &broken).await {
        tracing::warn!(
            "[2pc] could not rename corrupt wal segment {} to {}: {}",
            path.display(),
            broken.display(),
            rename_err
        );
    }
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
            }),
        );
        apply(
            &mut w,
            Record::Begin(BeginPayload {
                txn: txn(2),
                user: "u2".into(),
                database: "d2".into(),
            }),
        );
        apply(
            &mut w,
            Record::Checkpoint(CheckpointPayload {
                active: vec![CheckpointEntry {
                    txn: txn(99),
                    user: "u99".into(),
                    database: "d99".into(),
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
