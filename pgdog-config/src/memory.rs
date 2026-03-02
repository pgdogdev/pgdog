use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

/// Memory settings manage buffer allocations that PgDog uses during network I/O operations and task execution.
///
/// https://docs.pgdog.dev/configuration/pgdog.toml/memory/
#[derive(JsonSchema, Serialize, Deserialize, Debug, Clone, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct Memory {
    /// Size of the network read buffer in bytes. This buffer is used for reading data from client and server connections.
    ///
    /// _Default:_ `4096`
    ///
    /// https://docs.pgdog.dev/configuration/pgdog.toml/memory/#net_buffer
    #[serde(default = "default_net_buffer")]
    pub net_buffer: usize,

    /// Size of the message buffer in bytes. This buffer is used for assembling PostgreSQL protocol messages.
    ///
    /// _Default:_ `4096`
    ///
    /// https://docs.pgdog.dev/configuration/pgdog.toml/memory/#message_buffer
    #[serde(default = "default_message_buffer")]
    pub message_buffer: usize,

    /// Stack size for Tokio tasks in bytes. Increase this if you encounter stack overflow errors with complex queries.
    ///
    /// _Default:_ `2097152`
    ///
    /// https://docs.pgdog.dev/configuration/pgdog.toml/memory/#stack_size
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
