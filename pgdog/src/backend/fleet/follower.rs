//! A follower's live link to a coordination medium: NOTIFY
//! subscription (best effort; the fallback poll covers its absence),
//! registration heartbeat, and state/ack access.

use pgdog_config::Role;
use tokio::select;
use tokio::sync::broadcast::error::RecvError;
use tokio::time::{Instant, sleep};
use tracing::debug;

use crate::backend::fleet::protocol::{self, FALLBACK_POLL, StateRow, Topic};
use crate::backend::fleet::registry;
use crate::backend::pub_sub::PubSubListener;
use crate::backend::pub_sub::listener::Listener;
use crate::backend::{Cluster, Error};

/// Follows a topic on a medium. Owns the cluster it's given, and the
/// standalone LISTEN connection alongside it.
pub(crate) struct Follower {
    cluster: Cluster,
    topic: Topic,
    pub_sub: PubSubListener,
    notifications: Option<Listener>,
    last_beat: Option<Instant>,
}

impl Follower {
    /// Takes ownership of `cluster`; shuts it down on failure.
    pub(crate) async fn connect(cluster: Cluster, topic: Topic) -> Result<Self, Error> {
        let shard = match cluster.shards().first() {
            Some(shard) => shard,
            None => {
                cluster.shutdown();
                return Err(crate::backend::pool::Error::NoShard(0).into());
            }
        };
        let primary = shard
            .pools_with_roles()
            .into_iter()
            .find(|(role, _)| *role == Role::Primary)
            .map(|(_, pool)| pool);
        let Some(primary) = primary else {
            cluster.shutdown();
            return Err(crate::backend::pool::Error::NoShard(0).into());
        };

        let pub_sub = PubSubListener::new(&primary, shard.identifier(), 0);
        pub_sub.launch();
        let notifications = match pub_sub.listen(&topic.channel()).await {
            Ok(listener) => Some(listener),
            Err(err) => {
                debug!(
                    r#"follower of "{}" runs without NOTIFY: {}"#,
                    topic.as_str(),
                    err
                );
                None
            }
        };

        Ok(Self {
            cluster: cluster.clone(),
            topic,
            pub_sub,
            notifications,
            last_beat: None,
        })
    }

    pub(crate) fn shutdown(self) {
        self.pub_sub.shutdown();
        self.cluster.shutdown();
    }

    /// Ensure the fleet tables and re-register this instance, paced to
    /// the heartbeat interval (a no-op between beats). Registration
    /// doubles as the heartbeat a coordinator's completeness check
    /// reads, and re-ensuring the tables heals a wiped medium.
    pub(crate) async fn heartbeat(&mut self) -> Result<(), Error> {
        if self
            .last_beat
            .map(|beat| beat.elapsed() >= registry::HEARTBEAT_INTERVAL)
            .unwrap_or(true)
        {
            protocol::ensure_tables(&self.cluster).await?;
            registry::register(&self.cluster, 0).await?;
            self.last_beat = Some(Instant::now());
        }
        Ok(())
    }

    /// The coordinator's current state for this topic.
    pub(crate) async fn state(&self) -> Result<Option<StateRow>, Error> {
        protocol::read_state(&self.cluster, self.topic).await
    }

    /// Record this instance's ack for a state.
    pub(crate) async fn ack(&self, epoch: i64, state: &str) -> Result<(), Error> {
        protocol::ack(&self.cluster, self.topic, epoch, state).await
    }

    /// Sleep until the next state change or the fallback poll,
    /// whichever comes first.
    pub(crate) async fn wake(&mut self) {
        let Some(notifications) = self.notifications.as_mut() else {
            sleep(FALLBACK_POLL).await;
            return;
        };

        select! {
            _ = sleep(FALLBACK_POLL) => {}
            notification = notifications.recv() => {
                if let Err(RecvError::Closed) = notification {
                    // Never busy-loop on a closed channel; the poll
                    // takes over. Lagged is fine: we only need a wake.
                    self.notifications = None;
                }
            }
        }
    }
}
