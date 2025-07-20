pub mod hot_standby_feedback;
pub mod keep_alive;
pub mod logical;
pub mod status_update;
pub mod xlog_data;

use bytes::BytesMut;
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

impl ToBytes for ReplicationMeta {
    fn to_bytes(&self) -> Result<Bytes, Error> {
        let mut payload = BytesMut::new();
        // payload.put_u8(match self {
        //     Self::HotStandbyFeedback(_) => 'h' as u8,
        //     Self::StatusUpdate(_) => 'r' as u8,
        //     Self::KeepAlive(_) => 'k' as u8,
        // });

        payload.put(match self {
            Self::HotStandbyFeedback(hot) => hot.to_bytes()?,
            Self::StatusUpdate(status) => status.to_bytes()?,
            Self::KeepAlive(ka) => ka.to_bytes()?,
        });

        Ok(payload.freeze())
    }
}
