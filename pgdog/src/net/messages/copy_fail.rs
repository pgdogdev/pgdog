use super::{code, prelude::*};

#[derive(Debug, Clone)]
pub struct CopyFail {
    error: Bytes,
}

impl CopyFail {
    pub fn new(error: impl ToString) -> Self {
        let error = error.to_string();
        Self {
            error: Bytes::from(format!("{}\0", error)),
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
    fn to_bytes(&self) -> Result<Bytes, Error> {
        let mut payload = Payload::named(self.code());
        payload.put(self.error.clone());
        Ok(payload.freeze())
    }
}

impl Protocol for CopyFail {
    fn code(&self) -> char {
        'f'
    }
}

impl CopyFail {
    pub fn len(&self) -> usize {
        self.error.len() + 4
    }
}
