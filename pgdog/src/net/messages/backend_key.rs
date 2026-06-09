//! BackendKeyData (B) message.
//!
//! Direction-agnostic wire bundle: `pid` is a `FrontendPid` value in the client
//! direction and a real Postgres backend pid in the server direction. Never used
//! as a map key — all keying uses `FrontendPid` or `BackendPid` directly.

use crate::net::messages::code;
use crate::net::messages::prelude::*;
use crate::net::messages::protocol_version::ProtocolVersion;
use bytes::Buf;
use smallvec::SmallVec;

use super::frontend_pid::FrontendPid;

use rand::Rng;
const LEGACY_SECRET_LEN: usize = std::mem::size_of::<i32>();
const EXTENDED_SECRET_LEN: usize = 32;
const MAX_SECRET_LEN: usize = 256;

/// Variable-length cancel secret.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SecretKey {
    bytes: SmallVec<[u8; EXTENDED_SECRET_LEN]>,
}

impl SecretKey {
    /// 3.0-compatible secret from a 4-byte integer.
    pub fn legacy(secret: i32) -> Self {
        Self {
            bytes: SmallVec::from_slice(&secret.to_be_bytes()),
        }
    }

    pub fn random(len: usize) -> Self {
        assert!(
            (1..=MAX_SECRET_LEN).contains(&len),
            "cancel secret must be between 1 and {MAX_SECRET_LEN} bytes"
        );

        let mut bytes = SmallVec::with_capacity(len);
        bytes.resize(len, 0);
        rand::rng().fill(bytes.as_mut_slice());
        Self { bytes }
    }

    pub fn from_slice(secret: &[u8]) -> Result<Self, crate::net::Error> {
        if secret.is_empty() || secret.len() > MAX_SECRET_LEN {
            return Err(crate::net::Error::UnexpectedPayload);
        }

        Ok(Self {
            bytes: SmallVec::from_slice(secret),
        })
    }

    pub fn as_slice(&self) -> &[u8] {
        self.bytes.as_slice()
    }

    /// Compare two secrets in constant time.
    ///
    /// Cancel authentication compares an attacker-supplied secret against the
    /// stored one. A short-circuiting `==` would leak, via timing, how many
    /// leading bytes matched, letting an attacker recover the secret byte by
    /// byte. Length is not secret, so an early length mismatch returning `false`
    /// is fine.
    pub fn constant_time_eq(&self, other: &SecretKey) -> bool {
        crate::util::constant_time_eq(self.as_slice(), other.as_slice())
    }

    pub fn len(&self) -> usize {
        self.bytes.len()
    }
}

/// BackendKeyData (B) — pid + cancel secret on the wire.
/// `pid` is a raw `i32`; `from_bytes` must not mint a seq to keep round-trip pure.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct BackendKeyData {
    pub pid: i32,
    pub secret: SecretKey,
}

impl BackendKeyData {
    /// Wire pid.
    pub fn pid(&self) -> i32 {
        self.pid
    }

    /// Mint a key for a new client connection.
    pub fn new_frontend(protocol_version: ProtocolVersion, frontend_key: FrontendPid) -> Self {
        let secret_len = if protocol_version.supports_extended_cancel_key() {
            EXTENDED_SECRET_LEN
        } else {
            LEGACY_SECRET_LEN
        };

        Self {
            pid: frontend_key.pid(),
            secret: SecretKey::random(secret_len),
        }
    }

    /// Fallback for servers that don't send a `K` message (RDS-proxy etc.).
    /// `pid = 0` sentinel; cancel is a no-op for these connections.
    pub fn random_legacy() -> Self {
        Self {
            pid: 0,
            secret: SecretKey::random(LEGACY_SECRET_LEN),
        }
    }

    pub fn legacy(pid: i32, secret: i32) -> Self {
        Self {
            pid,
            secret: SecretKey::legacy(secret),
        }
    }
}

impl ToBytes for BackendKeyData {
    fn to_bytes(&self) -> bytes::Bytes {
        let mut payload = Payload::named(self.code());

        payload.put_i32(self.pid);
        payload.put_slice(self.secret.as_slice());

        payload.freeze()
    }
}

impl FromBytes for BackendKeyData {
    fn from_bytes(mut bytes: Bytes) -> Result<Self, Error> {
        code!(bytes, 'K');

        let len = bytes.get_i32();
        // Protocol 3.2 extended BackendKeyData with a variable-length secret;
        // 3.0 keeps the legacy 4-byte payload.
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
    use crate::net::FrontendPid;
    use crate::net::messages::{FromBytes, ToBytes};

    #[test]
    fn test_backend_key_roundtrip_legacy() {
        let key = BackendKeyData::legacy(42, 1234);
        let roundtrip = BackendKeyData::from_bytes(key.to_bytes()).unwrap();
        assert_eq!(roundtrip, key);
        assert_eq!(roundtrip.secret.len(), 4);
    }

    #[test]
    fn test_backend_key_roundtrip_extended() {
        let key = BackendKeyData {
            pid: 7,
            secret: SecretKey::random(32),
        };
        let roundtrip = BackendKeyData::from_bytes(key.to_bytes()).unwrap();
        assert_eq!(roundtrip, key);
        assert_eq!(roundtrip.secret.len(), 32);
    }

    #[test]
    fn test_backend_key_roundtrip_max_secret_len() {
        let key = BackendKeyData {
            pid: 9,
            secret: SecretKey::random(256),
        };
        let roundtrip = BackendKeyData::from_bytes(key.to_bytes()).unwrap();
        assert_eq!(roundtrip, key);
        assert_eq!(roundtrip.secret.len(), 256);
    }

    #[test]
    fn test_new_client_uses_protocol_specific_secret_length() {
        assert_eq!(
            BackendKeyData::new_frontend(ProtocolVersion::V3_0, FrontendPid::new())
                .secret
                .len(),
            4
        );
        assert_eq!(
            BackendKeyData::new_frontend(ProtocolVersion::V3_2, FrontendPid::new())
                .secret
                .len(),
            32
        );
    }
}
