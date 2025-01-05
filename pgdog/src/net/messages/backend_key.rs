//! BackendKeyData (B) message.

use crate::net::messages::code;
use crate::net::messages::prelude::*;
use rand::Rng;

/// BackendKeyData (B)
#[derive(Copy, Clone, Debug, Hash, PartialEq, Eq)]
pub struct BackendKeyData {
    /// Process ID.
    pub pid: i32,
    /// Process secret.
    pub secret: i32,
}

impl Default for BackendKeyData {
    fn default() -> Self {
        Self::new()
    }
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

impl FromBytes for BackendKeyData {
    fn from_bytes(mut bytes: Bytes) -> Result<Self, Error> {
        code!(bytes, 'K');

        let _len = bytes.get_i32();

        Ok(Self {
            pid: bytes.get_i32(),
            secret: bytes.get_i32(),
        })
    }
}

impl Protocol for BackendKeyData {
    fn code(&self) -> char {
        'K'
    }
}
