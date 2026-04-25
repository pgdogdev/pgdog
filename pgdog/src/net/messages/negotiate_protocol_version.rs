//! NegotiateProtocolVersion (B) message.

use bytes::{Buf, BufMut};

use crate::net::{c_string_buf, Error};

use super::{code, protocol_version::ProtocolVersion, FromBytes, Payload, Protocol, ToBytes};

/// NegotiateProtocolVersion (B)
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct NegotiateProtocolVersion {
    /// Protocol version chosen by the server.
    pub version: ProtocolVersion,
    /// Unsupported protocol option names from the startup packet.
    pub unrecognized_options: Vec<String>,
}

impl NegotiateProtocolVersion {
    /// Create a negotiation response.
    pub fn new(version: ProtocolVersion, unrecognized_options: Vec<String>) -> Self {
        Self {
            version,
            unrecognized_options,
        }
    }
}

impl ToBytes for NegotiateProtocolVersion {
    fn to_bytes(&self) -> Result<bytes::Bytes, Error> {
        let mut payload = Payload::named(self.code());
        payload.put_i32(self.version.as_i32());
        payload.put_i32(self.unrecognized_options.len() as i32);
        for option in &self.unrecognized_options {
            payload.put_string(option);
        }

        Ok(payload.freeze())
    }
}

impl FromBytes for NegotiateProtocolVersion {
    fn from_bytes(mut bytes: bytes::Bytes) -> Result<Self, Error> {
        code!(bytes, 'v');

        let _len = bytes.get_i32();
        let version = ProtocolVersion::from_i32(bytes.get_i32()).ok_or(Error::UnexpectedPayload)?;
        let count = bytes.get_i32();
        if count < 0 {
            return Err(Error::UnexpectedPayload);
        }

        let mut unrecognized_options = Vec::with_capacity(count as usize);
        for _ in 0..count {
            unrecognized_options.push(c_string_buf(&mut bytes));
        }

        Ok(Self {
            version,
            unrecognized_options,
        })
    }
}

impl Protocol for NegotiateProtocolVersion {
    fn code(&self) -> char {
        'v'
    }
}

#[cfg(test)]
mod tests {
    use super::{NegotiateProtocolVersion, ProtocolVersion};
    use crate::net::messages::{FromBytes, ToBytes};

    #[test]
    fn test_negotiate_protocol_version_roundtrip() {
        let message = NegotiateProtocolVersion::new(
            ProtocolVersion::V3_2,
            vec!["_pq_.command_tag".into(), "_pq_.other".into()],
        );

        let roundtrip = NegotiateProtocolVersion::from_bytes(message.to_bytes().unwrap()).unwrap();
        assert_eq!(roundtrip, message);
    }
}
