//! Healtcheck a connection.

use std::time::Duration;

use tokio::time::timeout;
use tokio::time::Instant;
use tracing::error;

use super::{Error, Pool};
use crate::backend::Server;

/// Perform a healtcheck on a connection.
pub struct Healtcheck<'a> {
    conn: &'a mut Server,
    pool: &'a Pool,
    healthcheck_interval: Duration,
    healthcheck_timeout: Duration,
    now: Instant,
}

impl<'a> Healtcheck<'a> {
    /// Perform a healtcheck only if necessary.
    pub fn conditional(
        conn: &'a mut Server,
        pool: &'a Pool,
        healthcheck_interval: Duration,
        healthcheck_timeout: Duration,
        now: Instant,
    ) -> Self {
        Self {
            conn,
            pool,
            healthcheck_interval,
            healthcheck_timeout,
            now,
        }
    }

    /// Perform a mandatory healtcheck.
    pub fn mandatory(conn: &'a mut Server, pool: &'a Pool, healthcheck_timeout: Duration) -> Self {
        Self::conditional(
            conn,
            pool,
            Duration::from_millis(0),
            healthcheck_timeout,
            Instant::now(),
        )
    }

    /// Perform the healtcheck if it's required.
    pub async fn healthcheck(&mut self) -> Result<(), Error> {
        let health_check_age = self.conn.healthcheck_age(self.now);

        if health_check_age < self.healthcheck_interval {
            return Ok(());
        }

        match timeout(self.healthcheck_timeout, self.conn.healthcheck(";")).await {
            Ok(Ok(())) => Ok(()),
            Ok(Err(err)) => {
                // Check if this is an administrator command termination
                if Self::is_admin_termination(&err) {
                    error!(
                        "connection terminated by administrator command [{}]",
                        self.pool.addr()
                    );
                } else {
                    error!("health check server error: {} [{}]", err, self.pool.addr());
                }
                Err(Error::HealthcheckError)
            }
            Err(_) => Err(Error::HealthcheckError),
        }
    }

    /// Check if the error is caused by administrator termination.
    fn is_admin_termination(err: &crate::backend::Error) -> bool {
        use crate::backend::Error;
        match err {
            Error::ExecutionError(error_response) => {
                error_response.code == "57P01"
                    && error_response
                        .message
                        .contains("terminating connection due to administrator command")
            }
            _ => false,
        }
    }
}
