use std::{ops::Deref, slice::from_raw_parts};

use crate::bindings::{PdDatabase, PdDatabases};

impl PdDatabase {
    pub fn replica(&self) -> bool {
        self.role == 1
    }

    pub fn primary(&self) -> bool {
        !self.replica()
    }

    pub fn shard(&self) -> u64 {
        self.shard
    }
}

impl Deref for PdDatabases {
    type Target = [PdDatabase];

    fn deref(&self) -> &Self::Target {
        unsafe { from_raw_parts(self.databases, self.len as usize) }
    }
}

impl From<&[PdDatabase]> for PdDatabases {
    fn from(value: &[PdDatabase]) -> Self {
        Self {
            databases: value.as_ptr() as *mut PdDatabase,
            len: value.len() as u64,
        }
    }
}
