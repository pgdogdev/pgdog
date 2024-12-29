//! Authentication successful message.

use crate::net::messages::{Payload, Protocol, ToBytes};
use bytes::BufMut;

// AuthenticationOk (F)
#[derive(Default)]
pub struct AuthenticationOk;

#[async_trait::async_trait]
impl Protocol for AuthenticationOk {
    fn code(&self) -> char {
        'R'
    }
}

impl ToBytes for AuthenticationOk {
    fn to_bytes(&self) -> Result<bytes::Bytes, crate::net::Error> {
        let mut payload = Payload::named(self.code());

        payload.put_i32(0);

        Ok(payload.freeze())
    }
}
