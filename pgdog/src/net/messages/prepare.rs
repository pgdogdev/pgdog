//! Used for preparing statements sent over with the simple protocol.

use std::str::from_utf8;

use pgdog_postgres_types::TypeName;

use super::prelude::*;
use super::{Message, Query};

#[derive(Debug, Clone, PartialEq)]
pub struct Prepare {
    pub(crate) name: Bytes,
    pub(crate) query: Bytes,
    pub(crate) data_types: Vec<TypeName>,
}

impl Prepare {
    pub fn query(&self) -> &str {
        from_utf8(&self.query).expect("prepare query to be utf-8")
    }

    pub fn name(&self) -> &str {
        from_utf8(&self.name).expect("prepare name to be utf-8")
    }

    pub fn len(&self) -> usize {
        self.name.len()
            + self.query.len()
            + self.data_types.iter().map(|t| t.0.len()).sum::<usize>()
    }
}

impl FromBytes for Prepare {
    fn from_bytes(bytes: Bytes) -> Result<Self, Error> {
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
        let query = from_utf8(&self.query).expect("prepare query to be utf-8");
        let name = from_utf8(&self.name).expect("prepare name to be utf-8");
        let data_types = if self.data_types.is_empty() {
            "".into()
        } else {
            format!(
                "({})",
                self.data_types
                    .iter()
                    .map(|data_type| data_type.to_string())
                    .collect::<Vec<_>>()
                    .join(", ")
            )
        };

        Query::new(format!(r#"PREPARE "{}" {} AS {}"#, name, data_types, query)).to_bytes()
    }
}
