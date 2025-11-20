use pgdog_config::Role;
use tokio::time::Instant;

use super::ReadTarget;

#[derive(Debug, Clone, Copy, Eq)]
pub struct DetectedRole {
    pub role: Role,
    pub as_of: Instant,
    pub database_number: usize,
}

impl DetectedRole {
    pub fn from_read_target(target: &ReadTarget) -> Self {
        Self {
            role: target.role,
            as_of: Instant::now(),
            database_number: target.pool.addr().database_number,
        }
    }
}

impl PartialEq for DetectedRole {
    fn eq(&self, other: &Self) -> bool {
        self.role == other.role && self.database_number == other.database_number
    }
}
