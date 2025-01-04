//! CommandComplete (B) message.

use crate::net::c_string_buf;

use super::code;
use super::prelude::*;

/// CommandComplete (B) message.
pub struct CommandComplete {
    /// Name of the command that was executed.
    pub command: String,
}

impl ToBytes for CommandComplete {
    fn to_bytes(&self) -> Result<Bytes, Error> {
        let mut payload = Payload::named(self.code());
        payload.put_string(&self.command);

        Ok(payload.freeze())
    }
}

impl FromBytes for CommandComplete {
    fn from_bytes(mut bytes: Bytes) -> Result<Self, Error> {
        code!(bytes, 'C');

        let _len = bytes.get_i32();
        let command = c_string_buf(&mut bytes);

        Ok(Self { command })
    }
}

impl Protocol for CommandComplete {
    fn code(&self) -> char {
        'C'
    }
}
