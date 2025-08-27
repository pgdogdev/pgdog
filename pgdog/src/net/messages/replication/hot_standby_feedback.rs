use bytes::BytesMut;

use super::super::code;
use super::super::prelude::*;

#[derive(Debug, Clone)]
pub struct HotStandbyFeedback {
    pub system_clock: i64,
    pub global_xmin: i32,
    pub epoch: i32,
    pub catalog_min: i32,
    pub epoch_catalog_min: i32,
}

impl FromBytes for HotStandbyFeedback {
    fn from_bytes(mut bytes: Bytes) -> Result<Self, Error> {
        code!(bytes, 'h');

        Ok(Self {
            system_clock: bytes.get_i64(),
            global_xmin: bytes.get_i32(),
            epoch: bytes.get_i32(),
            catalog_min: bytes.get_i32(),
            epoch_catalog_min: bytes.get_i32(),
        })
    }
}

impl ToBytes for HotStandbyFeedback {
    fn to_bytes(&self) -> Result<Bytes, Error> {
        let mut payload = BytesMut::new();
        payload.put_u8(b'h');
        payload.put_i64(self.system_clock);
        payload.put_i32(self.global_xmin);
        payload.put_i32(self.epoch);
        payload.put_i32(self.catalog_min);
        payload.put_i32(self.epoch_catalog_min);

        Ok(payload.freeze())
    }
}
