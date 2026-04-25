//! BackendKeyData (B) message.

use std::fmt::Display;
use std::sync::atomic::AtomicI32;
use std::sync::atomic::Ordering;

use crate::net::messages::code;
use crate::net::messages::prelude::*;
use crate::net::messages::protocol_version::ProtocolVersion;
use bytes::Buf;
use once_cell::sync::Lazy;
use rand::Rng;

static COUNTER: Lazy<AtomicI32> = Lazy::new(|| AtomicI32::new(0));
const LEGACY_SECRET_LEN: usize = std::mem::size_of::<i32>();
const EXTENDED_SECRET_LEN: usize = 32;
pub const MAX_SECRET_LEN: usize = 256;

// This wraps around.
fn next_counter() -> i32 {
    COUNTER.fetch_add(1, Ordering::SeqCst)
}

/// Variable-length cancel secret.
#[derive(Copy, Clone, Debug, Hash, PartialEq, Eq)]
pub struct SecretKey {
    len: u16,
    bytes: [u8; MAX_SECRET_LEN],
}

impl Default for SecretKey {
    fn default() -> Self {
        Self::legacy(0)
    }
}

impl SecretKey {
    /// Create a 3.0-compatible secret key from a 4-byte integer.
    pub fn legacy(secret: i32) -> Self {
        let mut bytes = [0; MAX_SECRET_LEN];
        bytes[..LEGACY_SECRET_LEN].copy_from_slice(&secret.to_be_bytes());
        Self {
            len: LEGACY_SECRET_LEN as u16,
            bytes,
        }
    }

    /// Create a random secret key of the requested length.
    pub fn random(len: usize) -> Self {
        assert!(
            (1..=MAX_SECRET_LEN).contains(&len),
            "cancel secret must be between 1 and {MAX_SECRET_LEN} bytes"
        );

        let mut bytes = [0; MAX_SECRET_LEN];
        rand::rng().fill(&mut bytes[..len]);
        Self {
            len: len as u16,
            bytes,
        }
    }

    /// Create a secret key from raw wire bytes.
    pub fn from_slice(secret: &[u8]) -> Result<Self, crate::net::Error> {
        if secret.is_empty() || secret.len() > MAX_SECRET_LEN {
            return Err(crate::net::Error::UnexpectedPayload);
        }

        let mut bytes = [0; MAX_SECRET_LEN];
        bytes[..secret.len()].copy_from_slice(secret);
        Ok(Self {
            len: secret.len() as u16,
            bytes,
        })
    }

    /// Secret bytes as they appear on the wire.
    pub fn as_slice(&self) -> &[u8] {
        &self.bytes[..self.len()]
    }

    /// Secret length in bytes.
    pub fn len(&self) -> usize {
        self.len as usize
    }
}

impl Display for SecretKey {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        if self.len() == LEGACY_SECRET_LEN {
            let legacy = i32::from_be_bytes(self.as_slice().try_into().expect("4-byte secret"));
            write!(f, "{legacy}")
        } else {
            for byte in self.as_slice() {
                write!(f, "{byte:02x}")?;
            }
            Ok(())
        }
    }
}

/// BackendKeyData (B)
#[derive(Copy, Clone, Debug, Hash, PartialEq, Eq, Default)]
pub struct BackendKeyData {
    /// Process ID.
    pub pid: i32,
    /// Process secret.
    pub secret: SecretKey,
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
            secret: SecretKey::random(LEGACY_SECRET_LEN),
        }
    }

    /// Create new BackendKeyData for a connected client.
    ///
    /// This counts client IDs incrementally.
    pub fn new_client(protocol_version: ProtocolVersion) -> Self {
        // The client must echo this secret back in CancelRequest, so its shape
        // has to match the negotiated frontend protocol version.
        let secret_len = if protocol_version == ProtocolVersion::V3_2 {
            EXTENDED_SECRET_LEN
        } else {
            LEGACY_SECRET_LEN
        };

        Self {
            pid: next_counter(),
            secret: SecretKey::random(secret_len),
        }
    }

    /// Create legacy 3.0-compatible backend key data.
    pub fn legacy(pid: i32, secret: i32) -> Self {
        Self {
            pid,
            secret: SecretKey::legacy(secret),
        }
    }
}

impl ToBytes for BackendKeyData {
    fn to_bytes(&self) -> Result<bytes::Bytes, crate::net::Error> {
        let mut payload = Payload::named(self.code());

        payload.put_i32(self.pid);
        payload.put_slice(self.secret.as_slice());

        Ok(payload.freeze())
    }
}

impl FromBytes for BackendKeyData {
    fn from_bytes(mut bytes: Bytes) -> Result<Self, Error> {
        code!(bytes, 'K');

        let len = bytes.get_i32();
        // Protocol 3.2 extends BackendKeyData with a variable-length secret,
        // while 3.0 keeps the legacy 4-byte payload.
        let secret_len = usize::try_from(len)
            .ok()
            .and_then(|len| len.checked_sub(8))
            .ok_or(Error::UnexpectedPayload)?;
        if secret_len == 0 || secret_len > MAX_SECRET_LEN || bytes.remaining() != 4 + secret_len {
            return Err(Error::UnexpectedPayload);
        }

        let pid = bytes.get_i32();
        let secret = SecretKey::from_slice(&bytes.copy_to_bytes(secret_len))?;

        Ok(Self { pid, secret })
    }
}

impl Protocol for BackendKeyData {
    fn code(&self) -> char {
        'K'
    }
}

#[cfg(test)]
mod tests {
    use super::{BackendKeyData, ProtocolVersion, SecretKey};
    use crate::net::messages::{FromBytes, ToBytes};

    #[test]
    fn test_backend_key_roundtrip_legacy() {
        let key = BackendKeyData::legacy(42, 1234);
        let roundtrip = BackendKeyData::from_bytes(key.to_bytes().unwrap()).unwrap();
        assert_eq!(roundtrip, key);
        assert_eq!(roundtrip.secret.len(), 4);
    }

    #[test]
    fn test_backend_key_roundtrip_extended() {
        let key = BackendKeyData {
            pid: 7,
            secret: SecretKey::random(32),
        };
        let roundtrip = BackendKeyData::from_bytes(key.to_bytes().unwrap()).unwrap();
        assert_eq!(roundtrip, key);
        assert_eq!(roundtrip.secret.len(), 32);
    }

    #[test]
    fn test_new_client_uses_protocol_specific_secret_length() {
        assert_eq!(
            BackendKeyData::new_client(ProtocolVersion::V3_0)
                .secret
                .len(),
            4
        );
        assert_eq!(
            BackendKeyData::new_client(ProtocolVersion::V3_2)
                .secret
                .len(),
            32
        );
    }
}
