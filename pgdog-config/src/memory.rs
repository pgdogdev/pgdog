use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

#[derive(JsonSchema, Serialize, Deserialize, Debug, Clone, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct Memory {
    #[serde(default = "default_net_buffer")]
    pub net_buffer: usize,
    #[serde(default = "default_message_buffer")]
    pub message_buffer: usize,
    #[serde(default = "default_stack_size")]
    pub stack_size: usize,
}

impl Default for Memory {
    fn default() -> Self {
        Self {
            net_buffer: default_net_buffer(),
            message_buffer: default_message_buffer(),
            stack_size: default_stack_size(),
        }
    }
}

fn default_net_buffer() -> usize {
    4096
}

fn default_message_buffer() -> usize {
    default_net_buffer()
}

// Default: 2MiB.
fn default_stack_size() -> usize {
    2 * 1024 * 1024
}
