//! Connection pool role,
//! powered by an atomic that allows
//! it to be changed without re-creating the struct.

use std::sync::{
    Arc,
    atomic::{AtomicU8, Ordering},
};

use pgdog_config::Role;

/// A pool/LB target role,
/// powered by an atomic so it's easy
/// to change it.
#[derive(Debug, Clone, Default)]
pub(crate) struct PoolRole {
    role: Arc<AtomicU8>,
}

impl PoolRole {
    /// Create new pool role.
    ///
    /// The role can be changed atomically.
    pub(crate) fn new(role: Role) -> Self {
        Self {
            role: Arc::new(AtomicU8::new(role.into())),
        }
    }

    /// Get current role value.
    pub(crate) fn role(&self) -> Role {
        let role = self.role.load(Ordering::Relaxed);
        role.try_into().expect("valid role")
    }

    /// Change role atomically, returning true if the role value changed.
    pub(crate) fn set_role(&self, role: Role) -> bool {
        self.role.swap(role.into(), Ordering::Relaxed) != u8::from(role)
    }
}
