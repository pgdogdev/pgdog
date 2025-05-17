//! CommandComplete (B) message.

use std::fmt::Debug;
use std::str::from_utf8;
use std::str::from_utf8_unchecked;

use super::code;
use super::prelude::*;

/// CommandComplete (B) message.
#[derive(Clone)]
pub struct CommandComplete {
    payload: Bytes,
}

impl Debug for CommandComplete {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("CommandComplete")
            .field("command", &self.command())
            .finish()
    }
}

impl CommandComplete {
    /// Number of rows sent/received.
    pub fn rows(&self) -> Result<Option<usize>, Error> {
        Ok(self
            .command()
            .split(" ")
            .last()
            .ok_or(Error::UnexpectedPayload)?
            .parse()
            .ok())
    }

    #[inline]
    pub(crate) fn command(&self) -> &str {
        unsafe { from_utf8_unchecked(&self.payload[5..self.payload.len() - 1]) }
    }

    pub(crate) fn from_str(s: &str) -> Self {
        let mut payload = Payload::named('C');
        payload.put_string(s);

        Self {
            payload: payload.freeze(),
        }
    }

    /// Rewrite the message with new number of rows.
    pub fn rewrite(&self, rows: usize) -> Result<Self, Error> {
        let mut parts = self.command().split(" ").collect::<Vec<_>>();
        parts.pop();
        let rows = rows.to_string();
        parts.push(rows.as_str());

        Ok(Self::from_str(&parts.join(" ")))
    }

    /// Start transaction.
    pub fn new_begin() -> Self {
        Self::from_str("BEGIN")
    }

    /// Rollback transaction.
    pub fn new_rollback() -> Self {
        Self::from_str("ROLLBACK")
    }

    /// Commit transaction.
    pub fn new_commit() -> Self {
        Self::from_str("COMMIT")
    }

    pub fn new(command: impl ToString) -> Self {
        Self::from_str(command.to_string().as_str())
    }
}

impl ToBytes for CommandComplete {
    fn to_bytes(&self) -> Result<Bytes, Error> {
        Ok(self.payload.clone())
    }
}

impl FromBytes for CommandComplete {
    fn from_bytes(mut bytes: Bytes) -> Result<Self, Error> {
        let original = bytes.clone();
        code!(bytes, 'C');

        // Check UTF-8!
        from_utf8(&original[5..original.len() - 1])?;

        Ok(Self { payload: original })
    }
}

impl Protocol for CommandComplete {
    fn code(&self) -> char {
        'C'
    }
}
