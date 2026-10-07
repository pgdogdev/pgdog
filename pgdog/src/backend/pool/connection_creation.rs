//! Connection creation primitives.

use std::{sync::Arc, time::Duration};
use tokio::time::Instant;

use crate::util::{safe_sleep, safe_timeout};
use tracing::error;

use super::{
    super::{ConnectReason, Oids, Pool, Server, ServerOptions},
    Address, Error,
};

pub(super) struct ConnectionArgs<'a> {
    address: &'a Address,
    timeout: Duration,
    attempts: u64,
    delay: Duration,
    options: ServerOptions,
    reason: ConnectReason,
    max_age: Duration,
    max_age_jitter: Duration,
    oids: Arc<Oids>,
    pool: &'a Pool,
}

impl<'a> ConnectionArgs<'a> {
    pub(super) fn from_pool(pool: &'a Pool, reason: ConnectReason) -> Self {
        Self {
            address: pool.addr(),
            timeout: pool.config().connect_timeout,
            attempts: pool.config().connect_attempts,
            delay: pool.config().connect_attempt_delay,
            options: pool.server_options(),
            reason,
            oids: pool.inner().oids.clone(),
            max_age: pool.config().max_age,
            max_age_jitter: pool.config().max_age_jitter,
            pool,
        }
    }

    pub(super) fn with_addr(self, addr: &'a Address) -> Self {
        Self {
            address: addr,
            ..self
        }
    }
}

pub(super) async fn create(args: ConnectionArgs<'_>) -> Result<Server, Error> {
    let ConnectionArgs {
        address,
        timeout,
        attempts,
        delay,
        options,
        reason,
        oids,
        max_age,
        max_age_jitter,
        pool,
    } = args;

    let mut error = Error::ServerError;
    let now = Instant::now();

    for attempt in 0..attempts {
        match safe_timeout(
            timeout,
            Box::pin(Server::connect(
                address,
                options.clone(),
                reason,
                oids.clone(),
            )),
        )
        .await
        {
            Ok(Ok(mut conn)) => {
                conn.stats_mut().set_pool_id(pool.id());
                let elapsed = now.elapsed();
                {
                    let mut guard = pool.lock();
                    guard.stats.counts.connect_count += 1;
                    guard.stats.counts.connect_time += elapsed;
                    guard.stats.counts.auth_attempts += conn.password_attempts();
                    conn.set_credentials_generation(guard.credentials_generation());
                }
                conn.apply_lifetime_jitter(max_age, max_age_jitter);
                pool.cache_params(conn.params());
                return Ok(conn);
            }

            Ok(Err(err)) => {
                // We tried all passwords and they were all wrong.
                if err.is_auth() {
                    pool.lock().stats.counts.auth_attempts += pool.addr().passwords.len();
                }
                error!(
                    "{}error connecting to server: {} [{}]",
                    if attempt > 0 {
                        format!("[attempt {}] ", attempt)
                    } else {
                        String::new()
                    },
                    err,
                    pool.addr(),
                );
                error = Error::ServerError;
            }

            Err(_) => {
                error!(
                    "{}server connection timeout [{}]",
                    if attempt > 0 {
                        format!("[attempt {}] ", attempt)
                    } else {
                        String::new()
                    },
                    pool.addr(),
                );
                error = Error::ConnectTimeout;
            }
        }

        safe_sleep(delay).await;
    }

    Err(error)
}
