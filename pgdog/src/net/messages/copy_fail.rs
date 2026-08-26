use super::{code, prelude::*};

#[derive(Debug, Clone, PartialEq)]
pub(crate) struct CopyFail {
    error: Bytes,
}

impl CopyFail {
    #[cfg(test)]
    pub(crate) fn new(error: impl AsRef<str>) -> Self {
        Self {
            error: super::c_string_bytes(error.as_ref()),
        }
    }
}

impl FromBytes for CopyFail {
    fn from_bytes(mut bytes: Bytes) -> Result<Self, Error> {
        code!(bytes, 'f');
        let _len = bytes.get_i32();

        Ok(Self { error: bytes })
    }
}

impl ToBytes for CopyFail {
    fn to_bytes(&self) -> Bytes {
        let mut payload = Payload::named(self.code());
        payload.put(self.error.clone());
        payload.freeze()
    }
}

impl Protocol for CopyFail {
    fn code(&self) -> char {
        'f'
    }
}

impl CopyFail {
    pub(crate) fn len(&self) -> usize {
        self.error.len() + 4
    }
}
