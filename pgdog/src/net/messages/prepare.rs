//! Used for preparing statements sent over with the simple protocol.
//!
//! This is a fake message. It contains a PREPARE query sent using the simple
//! protocol.

use std::str::from_utf8_unchecked;

pub static PREPARE_TEMPLATE_NAME: &str = "__pgdog_template_name";

use super::prelude::*;
use super::{Message, Query};

#[derive(Debug, Clone, PartialEq)]
pub struct Prepare {
    pub(crate) name: Bytes,
    pub(crate) query: Bytes,
}

impl Prepare {
    pub fn query(&self) -> &str {
        // SAFETY: We only support UTF-8.
        unsafe { from_utf8_unchecked(&self.query) }
    }

    pub fn name(&self) -> &str {
        // SAFETY: We only support UTF-8.
        unsafe { from_utf8_unchecked(&self.name) }
    }

    pub fn len(&self) -> usize {
        self.name.len() + self.query.len()
    }
}

impl FromBytes for Prepare {
    fn from_bytes(_bytes: Bytes) -> Result<Self, Error> {
        unreachable!("Prepare must be constructed manually")
    }
}

impl Protocol for Prepare {
    fn code(&self) -> char {
        'Q'
    }

    fn message(&self) -> Result<Message, Error> {
        Ok(Message::new(self.to_bytes()).frontend())
    }

    fn streaming(&self) -> bool {
        false
    }
}

impl ToBytes for Prepare {
    fn to_bytes(&self) -> Bytes {
        let query = self.query();
        let name = self.name();
        // This is safe because the statement looks like this:
        // PREPARE __pgdog_template_name AS [...]
        // so the template name will always match first.
        let query = query.replacen(PREPARE_TEMPLATE_NAME, name, 1);

        Query::new(query).to_bytes()
    }
}
