use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

use crate::Tcp;

#[derive(
    Serialize,
    Deserialize,
    Debug,
    Copy,
    Clone,
    PartialEq,
    Eq,
    Hash,
    Default,
    JsonSchema,
    derive_more::Display,
)]
#[serde(rename_all = "snake_case")]
#[display(rename_all = "snake_case")]
pub enum PostDataValidationStage {
    Off,
    #[default]
    DuringReplication,
    BeforeCutover,
    AfterCutover,
}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, JsonSchema)]
#[serde(rename_all = "snake_case")]
#[schemars(deny_unknown_fields)]
pub struct Resharding {
    #[serde(default)]
    pub post_data_validation: PostDataValidationStage,

    /// How long, in milliseconds, a table copy waits for a failed 2PC commit
    /// to be committed or rolled back. On timeout, the copy fails.
    /// _Default:_ `600000` (10 minutes)
    #[serde(default = "Resharding::two_pc_cleanup_timeout")]
    pub two_pc_cleanup_timeout: u64,

    /// How often, in milliseconds, a resharding state run
    /// is checked and the connection is updated.
    /// _Default:_ `5000` (5 seconds)
    #[serde(default = "Resharding::state_heartbeat_interval")]
    pub state_heartbeat_interval: u64,

    /// How long, in milliseconds, pgdog waits for one replication slot to drop
    /// when a resharding run cleans up. On timeout, the cleanup fails.
    /// _Default:_ `30000` (30 seconds)
    #[serde(default = "Resharding::replication_slot_drop_timeout")]
    pub replication_slot_drop_timeout: u64,

    /// How many times a failed resharding operation is retried. `0` retries
    /// forever.
    /// _Default:_ `10`
    #[serde(default = "Resharding::retry_max_attempts")]
    pub retry_max_attempts: usize,

    /// How long, in milliseconds, a failed resharding operation waits before
    /// each retry.
    /// _Default:_ `1000` (1 second)
    #[serde(default = "Resharding::retry_delay")]
    pub retry_delay: u64,

    /// TCP settings of all resharding connections: copy, replication and the
    /// resharding state.
    /// Fields not set here take these defaults:
    /// keep-alive after 60 s idle, every 15 s, 4 probes;
    /// `user_timeout` 120 s (2 minutes)
    #[serde(default)]
    tcp: Tcp,
}

impl Resharding {
    fn two_pc_cleanup_timeout() -> u64 {
        600_000
    }

    fn state_heartbeat_interval() -> u64 {
        5_000
    }

    fn replication_slot_drop_timeout() -> u64 {
        30_000
    }

    fn retry_max_attempts() -> usize {
        10
    }

    fn retry_delay() -> u64 {
        1_000
    }

    pub fn tcp(&self) -> Tcp {
        self.tcp.clone().with_defaults(Tcp {
            time: Some(60_000),
            interval: Some(15_000),
            retries: Some(4),
            user_timeout: Some(120_000),
            ..Tcp::default()
        })
    }
}

impl Default for Resharding {
    fn default() -> Self {
        Self {
            post_data_validation: PostDataValidationStage::default(),
            two_pc_cleanup_timeout: Self::two_pc_cleanup_timeout(),
            state_heartbeat_interval: Self::state_heartbeat_interval(),
            replication_slot_drop_timeout: Self::replication_slot_drop_timeout(),
            retry_max_attempts: Self::retry_max_attempts(),
            retry_delay: Self::retry_delay(),
            tcp: Tcp::default(),
        }
    }
}

#[cfg(test)]
mod test {
    use std::time::Duration;

    use super::*;

    #[test]
    fn partial_tcp_keeps_resharding_defaults() {
        let resharding: Resharding = toml::from_str("[tcp]\nuser_timeout = 30000").unwrap();
        let tcp = resharding.tcp();
        assert_eq!(tcp.user_timeout(), Some(Duration::from_secs(30)));
        assert_eq!(tcp.time(), Some(Duration::from_secs(60)));
        assert_eq!(tcp.interval(), Some(Duration::from_secs(15)));
        assert_eq!(tcp.retries(), Some(4));
        assert!(tcp.keepalive());
    }
}
