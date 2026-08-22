use super::code;
use super::prelude::*;

#[derive(Debug, Clone, PartialEq)]
pub struct Sync;

impl Default for Sync {
    fn default() -> Self {
        Self::new()
    }
}

impl Sync {
    pub fn len(&self) -> usize {
        5
    }

    pub fn new() -> Self {
        Self {}
    }
}

impl FromBytes for Sync {
    fn from_bytes(mut bytes: Bytes) -> Result<Self, Error> {
        code!(bytes, 'S');
        Ok(Sync)
    }
}

/// Sync has no payload, so its encoding is the constant 'S' + length 4.
static ENCODED: &[u8] = &[b'S', 0, 0, 0, 4];

impl ToBytes for Sync {
    fn to_bytes(&self) -> Bytes {
        Bytes::from_static(ENCODED)
    }
}

impl Protocol for Sync {
    fn code(&self) -> char {
        'S'
    }
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn test_sync() {
        assert_eq!(Sync.len(), Sync.to_bytes().len());
        assert_eq!(Sync.to_bytes(), Payload::named('S').freeze());
    }
}
