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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn hot_standby_feedback_roundtrip() {
        let feedback = HotStandbyFeedback {
            system_clock: 1234,
            global_xmin: 42,
            epoch: 7,
            catalog_min: 11,
            epoch_catalog_min: 13,
        };

        let bytes = feedback.to_bytes().expect("serialize hot standby feedback");
        let decoded = HotStandbyFeedback::from_bytes(bytes).expect("decode hot standby feedback");

        assert_eq!(decoded.system_clock, 1234);
        assert_eq!(decoded.global_xmin, 42);
        assert_eq!(decoded.epoch, 7);
        assert_eq!(decoded.catalog_min, 11);
        assert_eq!(decoded.epoch_catalog_min, 13);
    }
}
