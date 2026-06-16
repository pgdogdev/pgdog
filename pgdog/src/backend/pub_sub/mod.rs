pub mod client;
pub mod commands;
pub mod listener;
pub mod notification;
pub mod stats;

pub use client::PubSubClient;
pub use listener::PubSubListener;
pub use stats::{Stats, StatsSnapshot};

use crate::config::config;

fn channel_size() -> usize {
    std::cmp::max(1, config().config.general.pub_sub_channel_size)
}
