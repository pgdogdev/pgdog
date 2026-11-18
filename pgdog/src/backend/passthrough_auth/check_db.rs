use std::ops::Deref;
use std::sync::Arc;
use std::time::Duration;

use dashmap::DashMap;
use once_cell::sync::Lazy;
use pgdog_config::Config;
use pgdog_config::Role;
use pgdog_config::User;
use pgdog_config::users::PasswordKind;
use tokio::sync::OnceCell;
use tracing::error;

use crate::backend::pool::Error as PoolError;
use crate::backend::schema::SchemaCache;
use crate::backend::{Cluster, ConnectReason, DisconnectReason};

use crate::config::config;
use crate::util::safe_timeout;

use super::super::Error;
use super::super::databases::*;

#[cfg(test)]
mod test;

// (user, database) -> validator
static THROTTLE: Lazy<DashMap<Key, Arc<OnceCell<bool>>>> = Lazy::new(DashMap::default);

/// Remove all throttles and validation decisions.
///
/// Run this after reloading config to make sure passthrough auth re-validates
/// all users.
pub(in crate::backend) fn reset() {
    THROTTLE.clear();
}

#[derive(Hash, Eq, PartialEq, Clone)]
struct Key {
    inner: Arc<KeyInner>,
}

impl Deref for Key {
    type Target = KeyInner;

    fn deref(&self) -> &Self::Target {
        &self.inner
    }
}

#[derive(Hash, Eq, PartialEq, Clone)]
struct KeyInner {
    user: String,
    database: String,
    passwords: Vec<PasswordKind>,
}

impl Key {
    fn new(user: &User) -> Self {
        Self {
            inner: Arc::new(KeyInner {
                user: user.name.clone(),
                database: user.database.clone(),
                passwords: user.passwords(),
            }),
        }
    }
}

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
    key: Key,
}

impl Drop for ThrottleShutdown {
    //
    fn drop(&mut self) {
        use tokio::{spawn, time::sleep};
        let key = self.key.clone();
        spawn(async move {
            sleep(Duration::from_millis(
                config().config.general.passthrough_auth_debounce_delay,
            ))
            .await;
            THROTTLE.remove(&key);
        });
    }
}

/// Check if the credentials provided by the client are correct.
///
/// Protected against a thundering herd by a lock. The result of the check is preserved
/// for up to `passthrough_auth_debounce_delay`.
///
/// Run `callback` after successful validation while holding the lock.
pub(super) async fn check_db(user: &User, callback: impl FnOnce() -> Result<(), Error>) -> bool {
    let key = Key::new(user);
    let throttle = ThrottleShutdown { key };
    let entry = THROTTLE.entry(throttle.key.clone()).or_default().clone();
    let user: User = user.clone();

    entry
        .get_or_try_init(async || {
            let valid = check_password(&user, &config().config).await?;
            if valid {
                callback()?;
            }
            Ok::<_, Error>(valid)
        })
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

    let mut pools = cluster
        .shards()
        .first()
        .expect("cluster to have at least one shard")
        .pools_with_roles();
    pools.sort_by_key(|(role, _)| *role != Role::Primary);

    // Connect directly instead of checking out from a launched pool.
    // A pool doesn't pass connection errors to waiting clients, so a
    // rejected password would wait for the full checkout_timeout.
    let connect = async {
        for (_, pool) in pools {
            match pool.standalone(ConnectReason::PassthroughAuth).await {
                Ok(mut server) => {
                    server.disconnect_reason(DisconnectReason::CredentialsCheck);
                    return true;
                }
                // Replicas share the primary's roles and would reject it too.
                Err(PoolError::ServerAuth) => return false,
                Err(_) => continue,
            }
        }
        false
    };

    match safe_timeout(
        Duration::from_millis(config.general.checkout_timeout),
        connect,
    )
    .await
    {
        Err(_) | Ok(false) => {
            error!(
                r#"user "{}" could not connect to database "{}" to validate passthrough auth"#,
                cluster.user(),
                cluster.name(),
            );
            Ok(false)
        }

        Ok(true) => Ok(true),
    }
}
