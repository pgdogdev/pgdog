use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Copy)]
#[serde(deny_unknown_fields)]
pub struct Fdw {
    #[serde(default = "default_port")]
    pub port: u16,

    #[serde(default = "default_launch_timeout")]
    pub launch_timeout: u64,
}

impl Default for Fdw {
    fn default() -> Self {
        Self {
            port: default_port(),
            launch_timeout: default_launch_timeout(),
        }
    }
}

fn default_port() -> u16 {
    6433
}

fn default_launch_timeout() -> u64 {
    5_000
}
