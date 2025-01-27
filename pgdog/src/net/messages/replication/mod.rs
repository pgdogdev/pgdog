pub mod hot_standby_feedback;
pub mod keep_alive;
pub mod logical;
pub mod status_update;
pub mod xlog_data;

pub use hot_standby_feedback::HotStandbyFeedback;
pub use keep_alive::KeepAlive;
pub use logical::begin::Begin;
pub use logical::commit::Commit;
pub use logical::delete::Delete;
pub use logical::insert::Insert;
pub use logical::relation::Relation;
pub use logical::truncate::Truncate;
pub use logical::tuple_data::TupleData;
pub use logical::update::Update;
pub use status_update::StatusUpdate;
pub use xlog_data::XLogData;

use super::prelude::*;

#[derive(Debug, Clone)]
pub enum ReplicationMeta {
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
