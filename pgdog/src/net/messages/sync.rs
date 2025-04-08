use super::code;
use super::prelude::*;

#[derive(Debug, Clone)]
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

impl ToBytes for Sync {
    fn to_bytes(&self) -> Result<Bytes, Error> {
        Ok(Payload::named('S').freeze())
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
        assert_eq!(Sync.len(), Sync.to_bytes().unwrap().len());
    }
}
