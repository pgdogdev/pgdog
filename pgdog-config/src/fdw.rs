use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Copy)]
#[serde(deny_unknown_fields)]
pub struct Fdw {
    #[serde(default)]
    pub enabled: bool,

    #[serde(default = "default_green_port")]
    pub green_port: u16,

    #[serde(default = "default_blue_port")]
    pub blue_port: u16,

    #[serde(default = "default_launch_timeout")]
    pub launch_timeout: u64,
}

impl Default for Fdw {
    fn default() -> Self {
        Self {
            enabled: bool::default(),
            green_port: default_green_port(),
            blue_port: default_blue_port(),
            launch_timeout: default_launch_timeout(),
        }
    }
}

fn default_green_port() -> u16 {
    6433
}

fn default_blue_port() -> u16 {
    6434
}

fn default_launch_timeout() -> u64 {
    5_000
}
