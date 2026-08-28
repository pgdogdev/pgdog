pub(crate) mod hot_standby_feedback;
pub(crate) mod keep_alive;
pub(crate) mod logical;
pub(crate) mod status_update;
pub(crate) mod xlog_data;

pub(crate) use hot_standby_feedback::HotStandbyFeedback;
pub(crate) use keep_alive::KeepAlive;
pub(crate) use logical::commit::Commit;
pub(crate) use logical::delete::Delete;
pub(crate) use logical::insert::Insert;
pub(crate) use logical::relation::Relation;
pub(crate) use logical::tuple_data::TupleData;
pub(crate) use logical::update::UpdateIdentity;
pub(crate) use status_update::StatusUpdate;
pub(crate) use xlog_data::XLogData;

use super::prelude::*;

#[derive(Debug, Clone)]
pub(crate) enum ReplicationMeta {
    HotStandbyFeedback(HotStandbyFeedback),
    KeepAlive(KeepAlive),
    StatusUpdate(StatusUpdate),
}

impl FromBytes for ReplicationMeta {
    fn from_bytes(bytes: bytes::Bytes) -> Result<Self, Error> {
        Ok(match bytes[0] as char {
            'h' => Self::HotStandbyFeedback(HotStandbyFeedback::from_bytes(bytes)?),
            'r' => Self::StatusUpdate(StatusUpdate::from_bytes(bytes)?),
            'k' => Self::KeepAlive(KeepAlive::from_bytes(bytes)?),
            c => return Err(Error::UnexpectedReplicationMetaMessage(c)),
        })
    }
}

impl ToBytes for ReplicationMeta {
    fn to_bytes(&self) -> Bytes {
        match self {
            Self::HotStandbyFeedback(hot) => hot.to_bytes(),
            Self::StatusUpdate(status) => status.to_bytes(),
            Self::KeepAlive(ka) => ka.to_bytes(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn replication_meta_roundtrip_variants() {
        let feedback = HotStandbyFeedback {
            system_clock: 1,
            global_xmin: 2,
            epoch: 3,
            catalog_min: 4,
            epoch_catalog_min: 5,
        };
        let keepalive = KeepAlive {
            wal_end: 6,
            system_clock: 7,
            reply: 0,
        };
        let status = StatusUpdate {
            last_written: 8,
            last_flushed: 9,
            last_applied: 10,
            system_clock: 11,
            reply: 1,
        };

        for meta in [
            ReplicationMeta::HotStandbyFeedback(feedback.clone()),
            ReplicationMeta::KeepAlive(keepalive.clone()),
            ReplicationMeta::StatusUpdate(status.clone()),
        ] {
            let bytes = meta.to_bytes();
            let decoded = ReplicationMeta::from_bytes(bytes).expect("decode replication meta");
            match (meta, decoded) {
                (
                    ReplicationMeta::HotStandbyFeedback(expected),
                    ReplicationMeta::HotStandbyFeedback(actual),
                ) => {
                    assert_eq!(actual.system_clock, expected.system_clock);
                    assert_eq!(actual.global_xmin, expected.global_xmin);
                }
                (ReplicationMeta::KeepAlive(expected), ReplicationMeta::KeepAlive(actual)) => {
                    assert_eq!(actual.wal_end, expected.wal_end);
                    assert_eq!(actual.system_clock, expected.system_clock);
                }
                (
                    ReplicationMeta::StatusUpdate(expected),
                    ReplicationMeta::StatusUpdate(actual),
                ) => {
                    assert_eq!(actual.last_written, expected.last_written);
                    assert_eq!(actual.reply, expected.reply);
                }
                _ => panic!("replication meta variant mismatch"),
            }
        }
    }
}
