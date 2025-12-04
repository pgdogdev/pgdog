use super::Shard;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RoleChangeEvent {
    Failover,
    Initial,
    NoChange,
}

pub(super) struct RoleDetector {
    shard: Shard,
}

impl RoleDetector {
    /// Create new role change detector.
    pub(super) fn new(shard: &Shard) -> Self {
        Self {
            shard: shard.clone(),
        }
    }

    /// Detect role change in the shard.
    pub(super) fn changed(&mut self) -> bool {
        self.shard.redetect_roles()
    }
}
