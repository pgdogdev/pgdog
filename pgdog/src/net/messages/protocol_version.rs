//! PostgreSQL frontend/backend protocol versions.

use std::fmt::{Display, Formatter};

/// PostgreSQL protocol version.
#[derive(Copy, Clone, Debug, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct ProtocolVersion {
    major: u16,
    minor: u16,
}

impl ProtocolVersion {
    /// PostgreSQL protocol 3.0.
    pub const V3_0: Self = Self::new(3, 0);
    /// PostgreSQL protocol 3.2.
    pub const V3_2: Self = Self::new(3, 2);

    /// Create a new protocol version.
    pub const fn new(major: u16, minor: u16) -> Self {
        Self { major, minor }
    }

    /// Parse a protocol version from the wire representation.
    pub fn from_i32(raw: i32) -> Option<Self> {
        let raw = u32::try_from(raw).ok()?;
        let major = (raw >> 16) as u16;
        let minor = (raw & 0xFFFF) as u16;

        Some(Self::new(major, minor))
    }

    /// Encode this protocol version for the wire.
    pub fn as_i32(self) -> i32 {
        ((i32::from(self.major)) << 16) | i32::from(self.minor)
    }

    /// Protocol major version.
    pub const fn major(self) -> u16 {
        self.major
    }

    /// Protocol minor version.
    pub const fn minor(self) -> u16 {
        self.minor
    }

    /// Whether PgDog supports this protocol version directly.
    pub fn is_supported(self) -> bool {
        matches!(self, Self::V3_0 | Self::V3_2)
    }

    /// Highest supported protocol version that can satisfy this request.
    ///
    /// PostgreSQL minor-version negotiation is a downgrade mechanism, so a
    /// request for 3.1 negotiates to 3.0, while 3.3 negotiates to 3.2.
    pub fn negotiated(self) -> Option<Self> {
        if self.major != 3 {
            return None;
        }

        Some(match self.minor {
            0 | 1 => Self::V3_0,
            _ => Self::V3_2,
        })
    }
}

impl Display for ProtocolVersion {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}.{}", self.major, self.minor)
    }
}

#[cfg(test)]
mod tests {
    use super::ProtocolVersion;

    #[test]
    fn test_protocol_version_roundtrip() {
        let version = ProtocolVersion::V3_2;
        assert_eq!(ProtocolVersion::from_i32(version.as_i32()), Some(version));
    }

    #[test]
    fn test_protocol_version_negotiation() {
        assert_eq!(
            ProtocolVersion::V3_0.negotiated(),
            Some(ProtocolVersion::V3_0)
        );
        assert_eq!(
            ProtocolVersion::new(3, 1).negotiated(),
            Some(ProtocolVersion::V3_0)
        );
        assert_eq!(
            ProtocolVersion::V3_2.negotiated(),
            Some(ProtocolVersion::V3_2)
        );
        assert_eq!(
            ProtocolVersion::new(3, 3).negotiated(),
            Some(ProtocolVersion::V3_2)
        );
        assert_eq!(ProtocolVersion::new(4, 0).negotiated(), None);
    }
}
