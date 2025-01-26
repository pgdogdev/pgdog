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
