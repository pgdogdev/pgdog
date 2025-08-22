use std::time::Duration;

use crate::{config::General, frontend::ClientRequest, state::State};

#[derive(Debug, Clone, Copy)]
pub struct Timeouts {
    pub(super) query_timeout: Duration,
    pub(super) client_idle_timeout: Duration,
}

impl Default for Timeouts {
    fn default() -> Self {
        Self {
            query_timeout: Duration::MAX,
            client_idle_timeout: Duration::MAX,
        }
    }
}

impl Timeouts {
    pub(crate) fn from_config(general: &General) -> Self {
        Self {
            query_timeout: general.query_timeout(),
            client_idle_timeout: general.client_idle_timeout(),
        }
    }

    /// Get active query timeout.
    #[inline]
    pub(crate) fn query_timeout(&self, state: &State) -> Duration {
        match state {
            State::Active => self.query_timeout,
            _ => Duration::MAX,
        }
    }

    #[inline]
    pub(crate) fn client_idle_timeout(
        &self,
        state: &State,
        client_request: &ClientRequest,
    ) -> Duration {
        match state {
            State::Idle => {
                if client_request.messages.is_empty() {
                    self.client_idle_timeout
                } else {
                    Duration::MAX
                }
            }
            _ => Duration::MAX,
        }
    }
}
