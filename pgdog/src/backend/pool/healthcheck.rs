//! Healtcheck a connection.

use std::sync::Arc;
use std::time::{Duration, Instant};

use tokio::time::timeout;
use tracing::error;

use super::{Error, Pool};
use crate::backend::Server;

/// Perform a healtcheck on a connection.
pub struct Healtcheck<'a> {
    conn: &'a mut Server,
    pool: Pool,
    healthcheck_query: Arc<String>,
    healthcheck_interval: Duration,
    healthcheck_timeout: Duration,
}

impl<'a> Healtcheck<'a> {
    /// Perform a healtcheck only if necessary.
    pub fn conditional(
        conn: &'a mut Server,
        pool: Pool,
        healthcheck_query: Arc<String>,
        healthcheck_interval: Duration,
        healthcheck_timeout: Duration,
    ) -> Self {
        Self {
            conn,
            pool,
            healthcheck_query,
            healthcheck_interval,
            healthcheck_timeout,
        }
    }

    /// Perform a mandatory healtcheck.
    pub fn mandatory(
        conn: &'a mut Server,
        pool: Pool,
        healthcheck_query: Arc<String>,
        healthcheck_timeout: Duration,
    ) -> Self {
        Self::conditional(
            conn,
            pool,
            healthcheck_query,
            Duration::from_millis(0),
            healthcheck_timeout,
        )
    }

    /// Perform the healtcheck if it's required.
    pub async fn healthcheck(&mut self) -> Result<(), Error> {
        let healtcheck_age = self.conn.healthcheck_age(Instant::now());

        if healtcheck_age < self.healthcheck_interval {
            return Ok(());
        }

        match timeout(
            self.healthcheck_timeout,
            self.conn.healthcheck(&self.healthcheck_query),
        )
        .await
        {
            Ok(Ok(())) => Ok(()),
            Ok(Err(err)) => {
                error!("server error: {} [{}]", err, self.pool.addr());
                Err(Error::ServerError)
            }
            Err(_) => Err(Error::HealthcheckError),
        }
    }
}
