use std::{collections::HashMap, ops::Deref};

use super::Shard;
use crate::backend::pool::replicas::DetectedRole;

pub type DatabaseNumber = usize;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DetectedRoles {
    roles: HashMap<DatabaseNumber, DetectedRole>,
}

impl Deref for DetectedRoles {
    type Target = HashMap<DatabaseNumber, DetectedRole>;

    fn deref(&self) -> &Self::Target {
        &self.roles
    }
}

impl From<HashMap<DatabaseNumber, DetectedRole>> for DetectedRoles {
    fn from(value: HashMap<DatabaseNumber, DetectedRole>) -> Self {
        Self { roles: value }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RoleChangeEvent {
    Failover,
    Initial,
    NoChange,
}

pub(super) struct RoleDetector {
    current: DetectedRoles, // Database number <> Role
    shard: Shard,
}

impl RoleDetector {
    /// Create new role change detector.
    pub(super) fn new(shard: &Shard) -> Self {
        Self {
            current: shard.current_roles(),
            shard: shard.clone(),
        }
    }

    /// Detect role change in the shard.
    pub(super) fn changed(&mut self) -> bool {
        let latest = self.shard.redetect_roles();
        let mut changed = false;
        if let Some(latest) = latest {
            if self.current != latest {
                changed = true;
                self.current = latest;
            }
        }

        changed
    }
}
