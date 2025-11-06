use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize, Debug, Clone)]
#[serde(deny_unknown_fields)]
pub struct Memory {
    #[serde(default = "default_net_buffer")]
    pub net_buffer: usize,
    #[serde(default = "default_message_buffer")]
    pub message_buffer: usize,
}

impl Default for Memory {
    fn default() -> Self {
        Self {
            net_buffer: default_net_buffer(),
            message_buffer: default_message_buffer(),
        }
    }
}

fn default_net_buffer() -> usize {
    4096
}

fn default_message_buffer() -> usize {
    default_net_buffer()
}
