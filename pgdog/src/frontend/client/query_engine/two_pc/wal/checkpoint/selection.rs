//! Select closed segments without breaking retained WAL dependencies.

use std::io::{Error as IoError, ErrorKind};

use fnv::{FnvHashMap as HashMap, FnvHashSet as HashSet};

use super::{Error, Manager, Records, Segment, SegmentRegistry, TwoPcTransaction};

pub(super) struct SegmentDependencies {
    id: u64,
    transactions: HashSet<TwoPcTransaction>,
    identities: HashSet<TwoPcTransaction>,
    phases: HashSet<TwoPcTransaction>,
}

impl SegmentDependencies {
    pub(super) fn new(segment: Segment) -> Result<Self, Error> {
        let mut dependencies = Self {
            id: segment.segment_id,
            transactions: HashSet::default(),
            identities: HashSet::default(),
            phases: HashSet::default(),
        };
        for record in segment.records {
            let record = Records::try_from(record).map_err(|()| {
                IoError::new(
                    ErrorKind::InvalidData,
                    format!("invalid 2pc WAL record in segment {}", segment.segment_id),
                )
            })?;
            let transaction = match record {
                Records::Identity(record) => {
                    dependencies.identities.insert(record.transaction);
                    record.transaction
                }
                Records::Phase(record) => {
                    dependencies.phases.insert(record.transaction);
                    record.transaction
                }
                Records::Remove(record) => record.transaction,
            };
            dependencies.transactions.insert(transaction);
        }
        Ok(dependencies)
    }
}

/// Return removable segment IDs in crash-safe deletion order.
pub(super) fn candidates(segments: &[SegmentDependencies], manager: &Manager) -> Vec<u64> {
    let by_id: HashMap<_, _> = segments
        .iter()
        .map(|segment| (segment.id, segment))
        .collect();
    let identities: HashMap<_, _> = segments
        .iter()
        .flat_map(|segment| {
            segment
                .identities
                .iter()
                .map(move |transaction| (*transaction, segment.id))
        })
        .collect();

    SegmentRegistry::get().with_segment_to_transaction_mapping(|references| {
        // Take the active transaction snapshot while publication of queued
        // phase dependencies is locked. A transaction registers before writing
        // its identity and remains active until its final phase write finishes.
        // Thus a phase appended after selection either belongs to an active
        // transaction we retain here, or to a new transaction in a live segment.
        let active = manager.transactions();
        let mut pending = Vec::new();
        let mut retained = HashSet::default();

        for segment in segments {
            if segment
                .transactions
                .iter()
                .any(|tid| active.contains_key(tid))
            {
                pending.push(segment.id);
            }
        }

        // Everything outside the closed-segment snapshot must stay. Include
        // queued writes and segments that rotated while we read closed files.
        for (id, transactions) in references {
            if !by_id.contains_key(id) {
                pending.extend(transactions.iter().filter_map(|tid| identities.get(tid)));
            }
        }

        while let Some(id) = pending.pop() {
            if !retained.insert(id) {
                continue;
            }
            if let Some(segment) = by_id.get(&id) {
                pending.extend(segment.phases.iter().filter_map(|tid| identities.get(tid)));
            }
        }

        let mut candidates: Vec<_> = segments
            .iter()
            .filter(|segment| !retained.contains(&segment.id))
            .map(|segment| segment.id)
            .collect();
        candidates.sort_unstable_by(|a, b| b.cmp(a));
        candidates
    })
}
