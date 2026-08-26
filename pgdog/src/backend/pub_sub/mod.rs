pub(crate) mod client;
pub(crate) mod listener;
pub(crate) mod notification;
pub(crate) mod stats;

pub(crate) use client::PubSubClient;
pub(crate) use listener::PubSubListener;
pub(crate) use stats::{Stats, StatsSnapshot};

use crate::config::config;

fn channel_size() -> usize {
    std::cmp::max(1, config().config.general.pub_sub_channel_size)
}
