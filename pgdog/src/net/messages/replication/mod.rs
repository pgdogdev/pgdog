pub mod hot_standby_feedback;
pub mod keep_alive;
pub mod logical;
pub mod status_update;
pub mod xlog_data;

pub(crate) use hot_standby_feedback::HotStandbyFeedback;
pub(crate) use keep_alive::KeepAlive;
pub(crate) use logical::commit::Commit;
pub(crate) use logical::insert::Insert;
pub(crate) use logical::relation::Relation;
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
            _ => return Err(Error::UnexpectedPayload),
        })
    }
}

impl ToBytes for ReplicationMeta {
    fn to_bytes(&self) -> Result<Bytes, Error> {
        match self {
            Self::HotStandbyFeedback(hot) => hot.to_bytes(),
            Self::StatusUpdate(status) => status.to_bytes(),
            Self::KeepAlive(ka) => ka.to_bytes(),
        }
    }
}
