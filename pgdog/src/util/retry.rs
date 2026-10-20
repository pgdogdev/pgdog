use std::fmt::Display;
use std::time::Duration;

use tracing::{error, warn};

use super::safe_sleep;

/// Settings for [`Retry`].
#[derive(Debug, Clone, bon::Builder)]
pub(crate) struct RetryConfig {
    /// Shown in the log line of each retry.
    #[builder(into)]
    name: String,
    /// Attempts before giving up, `0` retries forever.
    max_attempts: usize,
    /// Sleep between attempts.
    delay: Duration,
}

/// Counts failed attempts and sleeps between them.
#[derive(Debug, Clone)]
pub(crate) struct Retry {
    config: RetryConfig,
    attempt: usize,
}

impl Retry {
    pub(crate) fn new(config: RetryConfig) -> Self {
        Self { config, attempt: 0 }
    }

    /// Start counting attempts from zero again.
    pub(crate) fn reset(&mut self) {
        self.attempt = 0;
    }

    /// Log `err` and sleep for the delay. When no attempts are left, log
    /// `err` as an error and return `false` without sleeping.
    #[must_use]
    pub(crate) async fn delay_retry(&mut self, err: &impl Display) -> bool {
        let RetryConfig {
            name,
            max_attempts,
            delay,
        } = &self.config;
        if *max_attempts != 0 && self.attempt >= *max_attempts {
            error!("[{name}] error after {max_attempts} retries: {err}, giving up");
            return false;
        }
        self.attempt += 1;
        warn!(
            "[{name}] error ({}/{max_attempts}): {err}, retrying in {}ms",
            self.attempt,
            delay.as_millis()
        );
        safe_sleep(*delay).await;
        true
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use tokio::time::Instant;

    const DELAY: Duration = Duration::from_millis(100);

    fn retry(max_attempts: usize) -> Retry {
        Retry::new(
            RetryConfig::builder()
                .name("test")
                .max_attempts(max_attempts)
                .delay(DELAY)
                .build(),
        )
    }

    #[tokio::test(start_paused = true)]
    async fn sleeps_until_attempts_run_out() {
        let mut retry = retry(2);

        for _ in 0..2 {
            let start = Instant::now();
            assert!(retry.delay_retry(&"boom").await);
            assert_eq!(start.elapsed(), DELAY);
        }

        let start = Instant::now();
        assert!(!retry.delay_retry(&"boom").await);
        assert_eq!(start.elapsed(), Duration::ZERO);
    }

    #[tokio::test(start_paused = true)]
    async fn zero_attempts_retries_forever() {
        let mut retry = retry(0);

        for _ in 0..100 {
            assert!(retry.delay_retry(&"boom").await);
        }
    }

    #[tokio::test(start_paused = true)]
    async fn reset_restores_attempts() {
        let mut retry = retry(1);

        assert!(retry.delay_retry(&"boom").await);
        assert!(!retry.delay_retry(&"boom").await);

        retry.reset();
        assert!(retry.delay_retry(&"boom").await);
        assert!(!retry.delay_retry(&"boom").await);
    }
}
