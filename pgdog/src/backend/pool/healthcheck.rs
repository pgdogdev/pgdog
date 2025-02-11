//! Healtcheck a connection.

use std::time::{Duration, Instant};

use tokio::time::timeout;
use tracing::error;

use super::{Error, Guard, Pool};

/// Perform a healtcheck on a connection.
pub struct Healtcheck {
    conn: Guard,
    pool: Pool,
    healthcheck_interval: Duration,
    healthcheck_timeout: Duration,
}

impl Healtcheck {
    /// Perform a healtcheck only if necessary.
    pub fn conditional(
        conn: Guard,
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

    /// Perform a mandatory healtcheck.
    pub fn mandatory(conn: Guard, pool: Pool, healthcheck_timeout: Duration) -> Self {
        Self::conditional(conn, pool, Duration::from_millis(0), healthcheck_timeout)
    }

    /// Perform the healtcheck if it's required.
    pub async fn healthcheck(mut self) -> Result<Guard, Error> {
        let healtcheck_age = self.conn.healthcheck_age(Instant::now());

        if healtcheck_age < self.healthcheck_interval {
            return Ok(self.conn);
        }

        match timeout(self.healthcheck_timeout, self.conn.healthcheck(";")).await {
            Ok(Ok(())) => Ok(self.conn),
            Ok(Err(err)) => {
                drop(self.conn); // Check the connection in first.
                self.pool.ban(Error::HealthcheckError);
                error!("server error: {} [{}]", err, self.pool.addr());
                Err(Error::ServerError)
            }
            Err(_) => {
                drop(self.conn); // Check the connection in first.
                self.pool.ban(Error::HealthcheckTimeout);
                Err(Error::HealthcheckError)
            }
        }
    }
}
