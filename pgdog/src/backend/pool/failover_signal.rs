#![allow(unused)]
use futures::StreamExt;
use futures::stream::FuturesUnordered;

use super::Address;
use super::shard::{Shard, failover_signal::*};

#[derive(Default, Clone, Debug)]
pub(crate) struct ClusterFailoverSignalWatcher {
    shards: Vec<FailoverSignalWatcher>,
}

impl ClusterFailoverSignalWatcher {
    pub(super) fn new(shards: &[Shard]) -> Self {
        Self {
            shards: shards
                .iter()
                .map(|shard| shard.failover_listener())
                .collect(),
        }
    }

    /// Wait for any shard to trigger a failover.
    pub(crate) async fn watch_any(&mut self) {
        let mut futs = self
            .shards
            .iter_mut()
            .map(|shard| shard.recv())
            .collect::<FuturesUnordered<_>>();
        futs.next()
            .await
            .expect("recv_any called on empty shard vec")
    }

    /// Wait for _all_ shards to trigger a failover. This is used
    /// to detect failover event on all primaries.
    pub(crate) async fn watch_all(&mut self) {
        let mut futs = self
            .shards
            .iter_mut()
            .map(|shard| shard.recv())
            .collect::<FuturesUnordered<_>>();

        while let Some(addr) = futs.next().await {}
    }
}
