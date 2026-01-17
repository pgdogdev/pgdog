use std::ops::{Deref, DerefMut};

use bytes::Bytes;

#[derive(Debug, Clone, Eq, Hash, PartialEq)]
pub struct Data {
    pub data: Bytes,
    pub is_null: bool,
}

impl Deref for Data {
    type Target = Bytes;

    fn deref(&self) -> &Self::Target {
        &self.data
    }
}

impl DerefMut for Data {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.data
    }
}

impl From<Bytes> for Data {
    fn from(value: Bytes) -> Self {
        Self {
            data: value,
            is_null: false,
        }
    }
}

impl From<(Bytes, bool)> for Data {
    fn from(value: (Bytes, bool)) -> Self {
        Self {
            data: value.0,
            is_null: value.1,
        }
    }
}

impl Data {
    pub fn null() -> Self {
        Self {
            data: Bytes::new(),
            is_null: true,
        }
    }

    pub fn is_null(&self) -> bool {
        self.is_null
    }
}
