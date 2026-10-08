use std::ops::Deref;
use std::sync::Arc;
use std::time::Duration;

use dashmap::DashMap;
use once_cell::sync::Lazy;
use pgdog_config::Config;
use pgdog_config::User;
use tokio::sync::OnceCell;
use tracing::error;

use crate::backend::Cluster;
use crate::backend::pool::Request;
use crate::backend::schema::SchemaCache;
use crate::util::safe_timeout;

use super::Error;
use super::databases::*;

// (user, database) -> validator
static THROTTLE: Lazy<DashMap<(String, String), Arc<OnceCell<bool>>>> = Lazy::new(DashMap::default);

// Make sure we shut down the cluster when the check is complete.
struct ClusterShutdown {
    cluster: Cluster,
}

impl Deref for ClusterShutdown {
    type Target = Cluster;

    fn deref(&self) -> &Self::Target {
        &self.cluster
    }
}

impl Drop for ClusterShutdown {
    fn drop(&mut self) {
        self.cluster.shutdown();
    }
}

struct ThrottleShutdown {
    key: (String, String),
}

impl Drop for ThrottleShutdown {
    // This makes it cancel-safe, so the throttle result is always removed.
    // We don't need to store it forever, just to prevent a thundering herd.
    fn drop(&mut self) {
        THROTTLE.remove(&self.key);
    }
}

/// Check if the credentials provided by the client are correct.
///
/// Protected against a thundering herd by a lock.
pub async fn check(user: &User, config: &Config) -> bool {
    let throttle = ThrottleShutdown {
        key: (user.name.clone(), user.database.clone()),
    };

    let entry = THROTTLE.entry(throttle.key.clone()).or_default().clone();

    entry
        .get_or_try_init(async || check_password(user, config).await)
        .await
        .copied()
        .unwrap_or_default()
}

async fn check_password(user: &User, config: &Config) -> Result<bool, Error> {
    // We're okay using an empty schema cache here because we won't be fetching the schema
    // on cluster startup.
    let cluster = if let Some((_, cluster)) = new_pool(user, config, SchemaCache::default()) {
        ClusterShutdown { cluster }
    } else {
        return Ok(false);
    };

    // Only launch the pools and try to connect.
    // Don't fetch schema or gate access to the cluster in any way.
    cluster.launch_pools();

    // A connection checkout is sufficient, the pool can only
    // return a connection if it can connect to the database.
    match safe_timeout(
        Duration::from_millis(config.general.checkout_timeout),
        cluster
            .shards()
            .iter()
            .next()
            .expect("cluster to have at least one shard")
            .primary_or_replica(&Request::default()),
    )
    .await
    {
        Err(_) | Ok(Err(_)) => {
            error!(
                r#"user "{}" could not connect to database "{}" to validate passthrough auth"#,
                cluster.user(),
                cluster.name(),
            );
            Ok(false)
        }

        Ok(_) => Ok(true),
    }
}
