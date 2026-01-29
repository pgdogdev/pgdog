// Submodules
pub mod auth;
pub mod core;
pub mod data_types;
pub mod database;
pub mod error;
pub mod general;
pub mod memory;
pub mod networking;
pub mod overrides;
pub mod pooling;
pub mod replication;
pub mod rewrite;
pub mod sharding;
pub mod system_catalogs;
pub mod url;
pub mod users;
pub mod util;

pub use auth::{AuthType, PassthoughAuth};
pub use core::{Config, ConfigAndUsers};
pub use data_types::*;
pub use database::{
    Database, EnumeratedDatabase, LoadBalancingStrategy, ReadWriteSplit, ReadWriteStrategy, Role,
};
pub use error::Error;
pub use general::General;
pub use memory::*;
pub use networking::{MultiTenant, Tcp, TlsVerifyMode};
pub use overrides::Overrides;
pub use pooling::{PoolerMode, PreparedStatements};
pub use replication::*;
pub use rewrite::{Rewrite, RewriteMode};
pub use sharding::*;
pub use system_catalogs::system_catalogs;
pub use users::{Admin, Plugin, User, Users};

use std::time::Duration;

// Make sure all duration fields
// are at least TOML-serializable.
pub const MAX_DURATION: Duration = Duration::from_millis(i64::MAX as u64);

#[cfg(test)]
mod test {
    use std::time::Duration;

    use serde::Serialize;

    use crate::{ConfigAndUsers, MAX_DURATION};

    #[test]
    fn test_max_duration() {
        assert!(MAX_DURATION > Duration::from_hours(24 * 7 * 52 * 100)); // 100 years
        assert_eq!(MAX_DURATION.as_millis() as i64, i64::MAX);

        #[derive(Serialize)]
        struct SerTest {
            value: u64,
        }

        let instance = SerTest {
            value: MAX_DURATION.as_millis() as u64,
        };

        toml::to_string(&instance).unwrap();
    }

    #[test]
    fn test_default_config_serializable() {
        let config = ConfigAndUsers::default();
        toml::to_string(&config.config).unwrap();
        toml::to_string(&config.users).unwrap();
    }
}
