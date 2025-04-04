//! CopyData (F & B) message.
use std::str::from_utf8;

use super::code;
use super::prelude::*;
use super::replication::ReplicationMeta;
use super::replication::XLogData;

/// CopyData (F & B) message.
#[derive(Clone)]
pub struct CopyData {
    data: Bytes,
}

impl CopyData {
    pub fn len(&self) -> usize {
        self.data.len()
    }

    /// New copy data row.
    pub fn new(data: &[u8]) -> Self {
        Self {
            data: Bytes::copy_from_slice(data),
        }
    }

    /// New copy data from bytes.
    pub fn bytes(data: Bytes) -> Self {
        Self { data }
    }

    /// Get copy data.
    pub fn data(&self) -> &[u8] {
        &self.data[..]
    }

    /// Get XLogData message from body, if there is one.
    pub fn xlog_data(&self) -> Option<XLogData> {
        XLogData::from_bytes(self.data.clone()).ok()
    }

    pub fn replication_meta(&self) -> Option<ReplicationMeta> {
        ReplicationMeta::from_bytes(self.data.clone()).ok()
    }
}

impl std::fmt::Debug for CopyData {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        if let Some(xlog_data) = self.xlog_data() {
            f.debug_struct("CopyData")
                .field("xlog_data", &xlog_data)
                .finish()
        } else if let Some(meta) = self.replication_meta() {
            f.debug_struct("CopyData")
                .field("replication_meta", &meta)
                .finish()
        } else {
            let mut f = f.debug_struct("CopyData");
            let f = if let Ok(s) = from_utf8(self.data()) {
                f.field("data", &s)
            } else {
                f.field("data", &self.data())
            };
            f.finish()
        }
    }
}

impl FromBytes for CopyData {
    fn from_bytes(mut bytes: Bytes) -> Result<Self, Error> {
        code!(bytes, 'd');
        let _len = bytes.get_i32();

        Ok(Self { data: bytes })
    }
}

impl ToBytes for CopyData {
    fn to_bytes(&self) -> Result<Bytes, Error> {
        let mut payload = Payload::named(self.code());
        payload.put(self.data.clone());

        Ok(payload.freeze())
    }
}

impl Protocol for CopyData {
    fn code(&self) -> char {
        'd'
    }
}
