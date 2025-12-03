use std::{collections::BTreeMap, ops::Deref};

use super::Shard;
use crate::backend::pool::replicas::DetectedRole;

pub type DatabaseNumber = usize;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DetectedRoles {
    roles: BTreeMap<DatabaseNumber, DetectedRole>,
}

impl Deref for DetectedRoles {
    type Target = BTreeMap<DatabaseNumber, DetectedRole>;

    fn deref(&self) -> &Self::Target {
        &self.roles
    }
}

impl From<BTreeMap<DatabaseNumber, DetectedRole>> for DetectedRoles {
    fn from(value: BTreeMap<DatabaseNumber, DetectedRole>) -> Self {
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

#[cfg(test)]
mod tests {
    use super::*;
    use pgdog_config::Role;
    use tokio::time::Instant;

    #[test]
    fn test_detected_roles_equality_independent_of_insertion_order() {
        let now = Instant::now();

        let role_0 = DetectedRole {
            role: Role::Primary,
            as_of: now,
            database_number: 0,
        };
        let role_1 = DetectedRole {
            role: Role::Replica,
            as_of: now,
            database_number: 1,
        };
        let role_2 = DetectedRole {
            role: Role::Replica,
            as_of: now,
            database_number: 2,
        };

        // Insert in ascending order: 0, 1, 2
        let mut map_asc = BTreeMap::new();
        map_asc.insert(0, role_0);
        map_asc.insert(1, role_1);
        map_asc.insert(2, role_2);
        let roles_asc: DetectedRoles = map_asc.into();

        // Insert in descending order: 2, 1, 0
        let mut map_desc = BTreeMap::new();
        map_desc.insert(2, role_2);
        map_desc.insert(1, role_1);
        map_desc.insert(0, role_0);
        let roles_desc: DetectedRoles = map_desc.into();

        // Insert in random order: 1, 2, 0
        let mut map_rand = BTreeMap::new();
        map_rand.insert(1, role_1);
        map_rand.insert(2, role_2);
        map_rand.insert(0, role_0);
        let roles_rand: DetectedRoles = map_rand.into();

        assert_eq!(roles_asc, roles_desc);
        assert_eq!(roles_asc, roles_rand);
        assert_eq!(roles_desc, roles_rand);
    }
}
