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
    pub(crate) fn new(role: Role) -> Self {
        Self {
            role: Arc::new(AtomicU8::new(role.try_into().expect("valid role"))),
        }
    }

    pub(crate) fn role(&self) -> Role {
        let role = self.role.load(Ordering::Relaxed);
        role.try_into().expect("valid role")
    }

    pub(crate) fn set_role(&self, role: Role) -> bool {
        self.role
            .swap(role.try_into().expect("valid role"), Ordering::Relaxed)
            != u8::try_from(role).expect("valid role")
    }
}
