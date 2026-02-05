//! BackendKeyData (B) message.

use std::fmt::Display;
use std::sync::atomic::AtomicI32;
use std::sync::atomic::Ordering;

use crate::net::messages::code;
use crate::net::messages::prelude::*;
use once_cell::sync::Lazy;
use rand::Rng;

static COUNTER: Lazy<AtomicI32> = Lazy::new(|| AtomicI32::new(0));

// This wraps around.
fn next_counter() -> i32 {
    COUNTER.fetch_add(1, Ordering::SeqCst)
}

/// BackendKeyData (B)
#[derive(Copy, Clone, Debug, Hash, PartialEq, Eq, Default)]
pub struct BackendKeyData {
    /// Process ID.
    pub pid: i32,
    /// Process secret.
    pub secret: i32,
}

impl Display for BackendKeyData {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "pid={}, secret={}", self.pid, self.secret)
    }
}

impl BackendKeyData {
    /// Create new random BackendKeyData (B) message.
    pub fn new() -> Self {
        Self {
            pid: rand::rng().random(),
            secret: rand::rng().random(),
        }
    }

    /// Create new BackendKeyData for a connected client.
    ///
    /// This counts client IDs incrementally.
    pub fn new_client() -> Self {
        Self {
            pid: next_counter(),
            secret: rand::rng().random(),
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
