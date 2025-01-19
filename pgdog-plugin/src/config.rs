//! Config helpers.

use crate::bindings::*;
use std::alloc::{alloc, dealloc, Layout};
use std::ffi::{CStr, CString};
use std::ptr::copy;

impl DatabaseConfig {
    /// Create new database config.
    pub fn new(host: CString, port: u16, role: Role, shard: usize) -> Self {
        Self {
            shard: shard as i32,
            role,
            port: port as i32,
            host: host.into_raw(),
        }
    }

    /// Get host name.
    pub fn host(&self) -> &str {
        unsafe { CStr::from_ptr(self.host) }.to_str().unwrap()
    }

    /// Database port.
    pub fn port(&self) -> u16 {
        self.port as u16
    }

    /// Shard.
    pub fn shard(&self) -> usize {
        self.shard as usize
    }

    /// Is this a replica?
    pub fn replica(&self) -> bool {
        self.role == Role_REPLICA
    }

    /// Is this a primary?
    pub fn primary(&self) -> bool {
        !self.replica()
    }

    /// Deallocate this structure after use.
    ///
    /// # Safety
    ///
    /// This is not to be used by plugins.
    /// This is for internal pgDog usage only.
    pub(crate) unsafe fn deallocate(&self) {
        drop(unsafe { CString::from_raw(self.host) })
    }
}

impl Config {
    /// Create new config structure.
    pub fn new(name: CString, databases: &[DatabaseConfig], shards: usize) -> Self {
        let layout = Layout::array::<DatabaseConfig>(databases.len()).unwrap();
        let ptr = unsafe {
            let ptr = alloc(layout) as *mut DatabaseConfig;
            copy(databases.as_ptr(), ptr, databases.len());
            ptr
        };

        Self {
            num_databases: databases.len() as i32,
            databases: ptr,
            name: name.into_raw(),
            shards: shards as i32,
        }
    }

    /// Get database at index.
    pub fn database(&self, index: usize) -> Option<DatabaseConfig> {
        if index < self.num_databases as usize {
            Some(unsafe { *self.databases.add(index) })
        } else {
            None
        }
    }

    /// Get all databases in this configuration.
    pub fn databases(&self) -> Vec<DatabaseConfig> {
        (0..self.num_databases)
            .map(|i| self.database(i as usize).unwrap())
            .collect()
    }

    /// Number of shards.
    pub fn shards(&self) -> usize {
        self.shards as usize
    }

    /// Deallocate this structure.
    ///
    /// SAFETY: This is not to be used by plugins.
    /// # Safety
    ///
    /// This is for internal pgDog usage only.
    pub(crate) unsafe fn deallocate(&self) {
        self.databases().into_iter().for_each(|d| d.deallocate());

        let layout = Layout::array::<DatabaseConfig>(self.num_databases as usize).unwrap();
        unsafe { dealloc(self.databases as *mut u8, layout) };
        drop(unsafe { CString::from_raw(self.name) })
    }
}
