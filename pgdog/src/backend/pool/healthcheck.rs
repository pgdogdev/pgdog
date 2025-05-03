//! Healthcheck a connection.

use std::time::{Duration, Instant};

use tokio::time::timeout;
use tracing::error;

use super::{Error, Pool};
use crate::backend::Server;

/// Perform a healthcheck on a connection.
pub struct Healthcheck<'a> {
    conn: &'a mut Server,
    pool: Pool,
    healthcheck_interval: Duration,
    healthcheck_timeout: Duration,
}

impl<'a> Healthcheck<'a> {
    /// Perform a healthcheck only if necessary.
    pub fn conditional(
        conn: &'a mut Server,
        pool: Pool,
        healthcheck_interval: Duration,
        healthcheck_timeout: Duration,
    ) -> Self {
        Self {
            conn,
            pool,
            healthcheck_interval,
            healthcheck_timeout,
        }
    }

    /// Perform a mandatory healthcheck.
    pub fn mandatory(conn: &'a mut Server, pool: Pool, healthcheck_timeout: Duration) -> Self {
        Self::conditional(conn, pool, Duration::from_millis(0), healthcheck_timeout)
    }

    /// Perform the healthcheck if it's required.
    pub async fn healthcheck(&mut self) -> Result<(), Error> {
        let healthcheck_age = self.conn.healthcheck_age(Instant::now());

        if healthcheck_age < self.healthcheck_interval {
            return Ok(());
        }

        match timeout(self.healthcheck_timeout, self.conn.healthcheck(";")).await {
            Ok(Ok(())) => Ok(()),
            Ok(Err(err)) => {
                error!("server error: {} [{}]", err, self.pool.addr());
                Err(Error::ServerError)
            }
            Err(_) => Err(Error::HealthcheckError),
        }
    }
}
