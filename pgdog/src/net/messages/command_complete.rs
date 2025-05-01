//! CommandComplete (B) message.

use crate::net::c_string_buf;

use super::code;
use super::prelude::*;

/// CommandComplete (B) message.
#[derive(Clone, Debug)]
pub struct CommandComplete {
    /// Name of the command that was executed.
    command: String,
    /// Original payload.
    original: Option<Bytes>,
}

impl CommandComplete {
    /// Number of rows sent/received.
    pub fn rows(&self) -> Result<Option<usize>, Error> {
        Ok(self
            .command
            .split(" ")
            .last()
            .ok_or(Error::UnexpectedPayload)?
            .parse()
            .ok())
    }

    #[inline]
    pub(crate) fn len(&self) -> usize {
        self.command.len() + 1 + 1 + 4
    }

    #[inline]
    pub(crate) fn command(&self) -> &str {
        &self.command
    }

    /// Rewrite the message with new number of rows.
    pub fn rewrite(&self, rows: usize) -> Result<Self, Error> {
        let mut parts = self.command.split(" ").collect::<Vec<_>>();
        parts.pop();
        let rows = rows.to_string();
        parts.push(rows.as_str());

        Ok(Self {
            command: parts.join(" "),
            original: None,
        })
    }

    /// Start transaction.
    pub fn new_begin() -> Self {
        Self {
            command: "BEGIN".into(),
            original: None,
        }
    }

    /// Rollback transaction.
    pub fn new_rollback() -> Self {
        Self {
            command: "ROLLBACK".into(),
            original: None,
        }
    }

    /// Commit transaction.
    pub fn new_commit() -> Self {
        Self {
            command: "COMMIT".into(),
            original: None,
        }
    }

    pub fn new(command: impl ToString) -> Self {
        Self {
            command: command.to_string(),
            original: None,
        }
    }
}

impl ToBytes for CommandComplete {
    fn to_bytes(&self) -> Result<Bytes, Error> {
        if let Some(ref original) = self.original {
            return Ok(original.clone());
        }

        let mut payload = Payload::named(self.code());
        payload.reserve(self.len());
        payload.put_string(&self.command);

        Ok(payload.freeze())
    }
}

impl FromBytes for CommandComplete {
    fn from_bytes(mut bytes: Bytes) -> Result<Self, Error> {
        let original = bytes.clone();
        code!(bytes, 'C');

        let _len = bytes.get_i32();
        let command = c_string_buf(&mut bytes);

        Ok(Self {
            command,
            original: Some(original),
        })
    }
}

impl Protocol for CommandComplete {
    fn code(&self) -> char {
        'C'
    }
}
