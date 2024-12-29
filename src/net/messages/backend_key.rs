//! Backend key data.

use super::{Payload, Protocol, ToBytes};
use bytes::BufMut;
use rand::Rng;

/// BackendKeyData (B)
pub struct BackendKeyData {
    pid: i32,
    secret: i32,
}

impl BackendKeyData {
    /// Create new random BackendKeyData (B) message.
    pub fn new() -> Self {
        Self {
            pid: rand::thread_rng().gen(),
            secret: rand::thread_rng().gen(),
        }
    }
}

impl ToBytes for BackendKeyData {
    fn to_bytes(&self) -> Result<bytes::Bytes, crate::net::Error> {
        let mut payload = Payload::named(self.code());

        payload.put_i32(self.pid);
        payload.put_i32(self.secret);

        Ok(payload.freeze())
    }
}

impl Protocol for BackendKeyData {
    fn code(&self) -> char {
        'K'
    }
}
