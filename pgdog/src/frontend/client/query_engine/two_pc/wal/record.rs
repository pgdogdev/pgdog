//! Two-phase commit WAL record format.
//!
//! On-disk framing per record:
//!
//! ```text
//! +---------+---------+-----+----------------+
//! | u32 LE  | u32 LE  | u8  | rmp-serde body |
//! | bodylen | crc32c  | tag | payload        |
//! +---------+---------+-----+----------------+
//! ```
//!
//! - `bodylen` covers `tag + payload` (everything after the crc).
//! - `crc32c` is computed over `tag + payload` and catches torn writes
//!   and bit-rot.
//! - `tag` is a stable [`u8`] discriminant defined by [`Tag`]. New record
//!   kinds receive new tag values; **tag values should never be reused**.
//! - `payload` is a [`rmp_serde`] encoding of a per-variant payload struct,
//!   using field-name keys (`to_vec_named`) so fields can be added
//!   additively under a single tag.
//!
//! There are four record kinds (see [`Record`]). The two write-ahead
//! invariants that the [`super::Wal`] writer must honour:
//!
//! 1. [`Record::Begin`] is fsynced before any `PREPARE TRANSACTION` reaches
//!    a shard — otherwise a crash can leave prepared xacts with no record
//!    of them.
//! 2. [`Record::Committing`] is fsynced before any `COMMIT PREPARED` reaches
//!    a shard — otherwise a crash can leave a partial commit with no way to
//!    know the coordinator had decided to commit.
//!
//! [`Record::End`] lets recovery forget a finished txn and is not safety
//! critical. [`Record::Checkpoint`] is a snapshot of all active txns so
//! older segments can be garbage-collected.
//!
//! # Format evolution rules
//!
//! These rules keep the on-disk format stable across versions:
//!
//! - **Tag values are immutable.** Once assigned, a [`Tag`] discriminant
//!   should never be repurposed, even if the record kind is removed.
//! - **Fields may be added.** New fields on an existing variant's payload
//!   struct must carry `#[serde(default)]` so older versions that wrote
//!   records without the field can still be deserialized.
//! - **Fields may not be removed or renamed.** To change a variant's
//!   shape, introduce a new tag with a new payload struct and deprecate
//!   the old tag after a checkpoint window.
//! - **Type changes** on existing fields (e.g. `Vec<u32>` → `Vec<u64>`)
//!   also require a new tag.

use bytes::{Buf, BufMut};
use serde::{Deserialize, Serialize};

use super::error::Error;
use crate::frontend::client::query_engine::two_pc::TwoPcTransaction;

/// On-disk record-kind discriminant.
///
/// Values are stable; see the format-evolution rules in the module docs.
#[repr(u8)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Tag {
    /// Coordinator is about to issue `PREPARE TRANSACTION` to `shards`.
    Begin = 1,
    /// Coordinator has decided to commit; must drive all participants to
    /// `COMMIT PREPARED`.
    Committing = 2,
    /// Coordinator is done with this txn; recovery may forget it.
    End = 3,
    /// Snapshot of all active txns for log GC.
    Checkpoint = 4,
}

impl Tag {
    fn from_u8(b: u8) -> Result<Self, u8> {
        match b {
            1 => Ok(Self::Begin),
            2 => Ok(Self::Committing),
            3 => Ok(Self::End),
            4 => Ok(Self::Checkpoint),
            other => Err(other),
        }
    }
}

/// A single WAL record.
#[derive(Debug, Clone, PartialEq)]
pub enum Record {
    Begin(BeginPayload),
    Committing(TxnPayload),
    End(TxnPayload),
    Checkpoint(CheckpointPayload),
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct BeginPayload {
    pub txn: TwoPcTransaction,
    pub user: String,
    pub database: String,
}

/// Payload for records that carry only a transaction id
/// ([`Record::Committing`] and [`Record::End`]).
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct TxnPayload {
    pub txn: TwoPcTransaction,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct CheckpointPayload {
    pub active: Vec<CheckpointEntry>,
}

/// One row in a [`CheckpointPayload`].
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct CheckpointEntry {
    pub txn: TwoPcTransaction,
    pub user: String,
    pub database: String,
    /// `true` iff a [`Record::Committing`] had been fsynced for this txn.
    pub decided: bool,
}

/// A successfully decoded record and the number of bytes it consumed.
#[derive(Debug)]
pub struct Decoded {
    pub record: Record,
    pub consumed: usize,
}

const HEADER_BYTES: usize = 4 + 4;

impl Record {
    /// The on-disk tag for this record.
    pub fn tag(&self) -> Tag {
        match self {
            Record::Begin { .. } => Tag::Begin,
            Record::Committing { .. } => Tag::Committing,
            Record::End { .. } => Tag::End,
            Record::Checkpoint { .. } => Tag::Checkpoint,
        }
    }

    /// Append a framed copy of this record to `out`.
    pub fn encode<B: BufMut>(&self, out: &mut B) -> Result<(), Error> {
        let payload = match self {
            Record::Begin(p) => rmp_serde::to_vec_named(p)?,
            Record::Committing(p) => rmp_serde::to_vec_named(p)?,
            Record::End(p) => rmp_serde::to_vec_named(p)?,
            Record::Checkpoint(p) => rmp_serde::to_vec_named(p)?,
        };

        let tag = self.tag() as u8;
        let body_len = u32::try_from(1 + payload.len())
            .map_err(|_| Error::RecordTooLarge(1 + payload.len()))?;

        let mut crc = crc32c::crc32c(&[tag]);
        crc = crc32c::crc32c_append(crc, &payload);

        out.put_u32_le(body_len);
        out.put_u32_le(crc);
        out.put_u8(tag);
        out.put_slice(&payload);
        Ok(())
    }

    /// Try to decode the next record from `buf`.
    ///
    /// - `Ok(Some(d))` — a complete record was decoded; advance the cursor
    ///   by `d.consumed`.
    /// - `Ok(None)` — `buf` doesn't contain a complete record yet. Read
    ///   more bytes and call again. This is normal flow at end-of-segment.
    /// - `Err(_)` — the record is corrupt (bad framing, CRC mismatch,
    ///   unknown tag, or undecodable payload). The WAL stream effectively
    ///   ends here.
    pub fn decode(buf: &[u8]) -> Result<Option<Decoded>, Error> {
        if buf.len() < HEADER_BYTES {
            return Ok(None);
        }
        let mut hdr = &buf[..HEADER_BYTES];
        let body_len = hdr.get_u32_le() as usize;
        if body_len == 0 {
            return Err(Error::EmptyRecord);
        }
        let total = HEADER_BYTES + body_len;
        if buf.len() < total {
            return Ok(None);
        }
        let expected_crc = hdr.get_u32_le();
        let body = &buf[HEADER_BYTES..total];
        let actual_crc = crc32c::crc32c(body);
        if actual_crc != expected_crc {
            return Err(Error::Crc {
                expected: expected_crc,
                actual: actual_crc,
            });
        }
        let tag = Tag::from_u8(body[0]).map_err(Error::InvalidTag)?;
        let payload = &body[1..];
        let record = match tag {
            Tag::Begin => Record::Begin(rmp_serde::from_slice(payload)?),
            Tag::Committing => Record::Committing(rmp_serde::from_slice(payload)?),
            Tag::End => Record::End(rmp_serde::from_slice(payload)?),
            Tag::Checkpoint => Record::Checkpoint(rmp_serde::from_slice(payload)?),
        };
        Ok(Some(Decoded {
            record,
            consumed: total,
        }))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn round_trip(rec: Record) {
        let mut buf = Vec::new();
        rec.encode(&mut buf).unwrap();
        let d = Record::decode(&buf).unwrap().unwrap();
        assert_eq!(d.record, rec);
        assert_eq!(d.consumed, buf.len());
    }

    #[test]
    fn round_trip_begin() {
        round_trip(Record::Begin(BeginPayload {
            txn: TwoPcTransaction::new(),
            user: "alice".into(),
            database: "shop".into(),
        }));
    }

    #[test]
    fn round_trip_committing() {
        round_trip(Record::Committing(TxnPayload {
            txn: TwoPcTransaction::new(),
        }));
    }

    #[test]
    fn round_trip_end() {
        round_trip(Record::End(TxnPayload {
            txn: TwoPcTransaction::new(),
        }));
    }

    #[test]
    fn round_trip_checkpoint() {
        round_trip(Record::Checkpoint(CheckpointPayload {
            active: vec![
                CheckpointEntry {
                    txn: TwoPcTransaction::new(),
                    user: "u1".into(),
                    database: "d1".into(),
                    decided: false,
                },
                CheckpointEntry {
                    txn: TwoPcTransaction::new(),
                    user: "u2".into(),
                    database: "d2".into(),
                    decided: true,
                },
            ],
        }));
    }

    #[test]
    fn need_more_when_truncated() {
        let mut buf = Vec::new();
        Record::End(TxnPayload {
            txn: TwoPcTransaction::new(),
        })
        .encode(&mut buf)
        .unwrap();
        for prefix_len in 0..buf.len() {
            assert!(matches!(Record::decode(&buf[..prefix_len]), Ok(None)));
        }
    }

    #[test]
    fn crc_mismatch_is_error() {
        let mut buf = Vec::new();
        Record::End(TxnPayload {
            txn: TwoPcTransaction::new(),
        })
        .encode(&mut buf)
        .unwrap();
        let last = buf.len() - 1;
        buf[last] ^= 0xff;
        assert!(matches!(Record::decode(&buf), Err(Error::Crc { .. })));
    }

    #[test]
    fn unknown_tag_is_error() {
        let mut buf = Vec::new();
        Record::End(TxnPayload {
            txn: TwoPcTransaction::new(),
        })
        .encode(&mut buf)
        .unwrap();
        // Tag sits right after the 8-byte framing header.
        buf[HEADER_BYTES] = 99;
        // Re-stamp the CRC so we hit the InvalidTag branch, not Crc.
        let crc = crc32c::crc32c(&buf[HEADER_BYTES..]);
        buf[4..8].copy_from_slice(&crc.to_le_bytes());
        assert!(matches!(Record::decode(&buf), Err(Error::InvalidTag(99))));
    }

    #[test]
    fn multiple_records_decode_sequentially() {
        let mut buf = Vec::new();
        let a = Record::Begin(BeginPayload {
            txn: TwoPcTransaction::new(),
            user: "u".into(),
            database: "d".into(),
        });
        let b = Record::Committing(TxnPayload {
            txn: TwoPcTransaction::new(),
        });
        a.encode(&mut buf).unwrap();
        b.encode(&mut buf).unwrap();

        let d1 = Record::decode(&buf).unwrap().unwrap();
        assert_eq!(d1.record, a);
        let d2 = Record::decode(&buf[d1.consumed..]).unwrap().unwrap();
        assert_eq!(d2.record, b);
        assert_eq!(d1.consumed + d2.consumed, buf.len());
    }
}
