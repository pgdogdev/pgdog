//! Drives one coordination attempt over a medium: publishes states,
//! collects acks, keeps a held state fresh. One epoch per attempt.

use std::collections::HashSet;
use std::time::Duration;

use tokio::time::{Instant, interval};
use tracing::warn;

use crate::backend::fleet::protocol::{self, KEEPALIVE_INTERVAL, Topic};
use crate::backend::fleet::registry;
use crate::backend::{Cluster, Error};

/// The outcome of discovering the fleet before a coordination attempt.
pub(crate) enum Discovery {
    /// No live peers: coordinate with nobody.
    Solo,
    /// Live peers that haven't registered on the medium — they run a
    /// config that doesn't know it. Names inside, formatted for an
    /// error message.
    Missing(String),
    Ready(Box<Coordinator>),
}

/// One coordination attempt: this instance leading its live peers
/// through topic states published on the medium.
pub(crate) struct Coordinator {
    /// A clone of the caller's medium cluster (shared pools).
    medium: Cluster,
    peers: Vec<registry::Instance>,
    topic: Topic,
    epoch: i64,
    me: i64,
}

impl Coordinator {
    /// Read the fleet from the registry on shard 0 of `fleet`, and
    /// check every live peer against the registrations on the medium.
    pub(crate) async fn discover(
        topic: Topic,
        fleet: &Cluster,
        medium: &Cluster,
    ) -> Result<Discovery, Error> {
        let me = registry::node_id();
        let all = registry::live_instances(fleet, 0).await?;
        let peers = all
            .into_iter()
            .filter(|instance| instance.node_id != me)
            .collect::<Vec<_>>();
        if peers.is_empty() {
            return Ok(Discovery::Solo);
        }

        let registered = registry::live_instances(medium, 0)
            .await?
            .into_iter()
            .map(|instance| instance.node_id)
            .collect::<HashSet<_>>();

        let coordinator = Self {
            medium: medium.clone(),
            peers,
            topic,
            epoch: protocol::epoch(),
            me,
        };
        let missing = coordinator.stragglers(&registered);
        if missing.is_empty() {
            Ok(Discovery::Ready(Box::new(coordinator)))
        } else {
            Ok(Discovery::Missing(missing))
        }
    }

    pub(crate) fn medium(&self) -> &Cluster {
        &self.medium
    }

    /// The peers absent from `acks`, formatted for an error message.
    fn stragglers(&self, acks: &HashSet<i64>) -> String {
        self.peers
            .iter()
            .filter(|peer| !acks.contains(&peer.node_id))
            .map(|peer| format!("{} ({})", peer.node_id, peer.hostname))
            .collect::<Vec<_>>()
            .join(", ")
    }

    /// Publish a state and wait for every peer to ack it. `Ok(None)`
    /// when everyone acked; `Ok(Some(stragglers))` on deadline.
    pub(crate) async fn broadcast_and_await(
        &self,
        state: &str,
        timeout: Duration,
    ) -> Result<Option<String>, Error> {
        protocol::write_state(&self.medium, self.topic, state, self.epoch, self.me).await?;

        let deadline = Instant::now() + timeout;
        let mut poll = interval(Duration::from_millis(100));
        loop {
            poll.tick().await;
            let acks = protocol::acked(&self.medium, self.topic, self.epoch, state).await?;
            if self.peers.iter().all(|peer| acks.contains(&peer.node_id)) {
                return Ok(None);
            }
            if Instant::now() > deadline {
                return Ok(Some(self.stragglers(&acks)));
            }
        }
    }

    /// Publish a state without waiting for acks, best effort.
    pub(crate) async fn publish(&self, state: &str) {
        if let Err(err) =
            protocol::write_state(&self.medium, self.topic, state, self.epoch, self.me).await
        {
            warn!(
                r#"failed to publish "{}" for "{}": {}"#,
                state,
                self.topic.as_str(),
                err
            );
        }
    }

    /// Refresh `state` every [`KEEPALIVE_INTERVAL`] for as long as the
    /// guard lives, so followers' silence failsafe only fires on a
    /// coordinator that actually died. Drop the guard before
    /// publishing any other state for this epoch.
    pub(crate) fn keep_fresh(&self, state: &'static str) -> KeepFresh {
        let medium = self.medium.clone();
        let (topic, epoch, me) = (self.topic, self.epoch, self.me);
        KeepFresh(crate::tasks::spawn("fleet state keepalive", async move {
            let mut tick = interval(KEEPALIVE_INTERVAL);
            loop {
                tick.tick().await;
                let _ = protocol::write_state(&medium, topic, state, epoch, me).await;
            }
        }))
    }
}

/// Aborts the state keepalive on drop.
pub(crate) struct KeepFresh(tokio::task::JoinHandle<()>);

impl Drop for KeepFresh {
    fn drop(&mut self) {
        self.0.abort();
    }
}
