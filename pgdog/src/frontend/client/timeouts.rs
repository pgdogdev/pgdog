use std::time::Duration;

use crate::{config::ConfigAndUsers, frontend::ClientRequest, state::State};

#[derive(Debug, Clone, Copy)]
pub(crate) struct Timeouts {
    pub(super) query_timeout: Duration,
    pub(super) client_idle_timeout: Duration,
    pub(super) idle_in_transaction_timeout: Duration,
}

impl Default for Timeouts {
    fn default() -> Self {
        Self {
            query_timeout: Duration::MAX,
            client_idle_timeout: Duration::MAX,
            idle_in_transaction_timeout: Duration::MAX,
        }
    }
}

impl Timeouts {
    pub(crate) fn from_config(
        config: &ConfigAndUsers,
        user: &str,
        database: &str,
        admin: bool,
    ) -> Self {
        Self {
            query_timeout: config.config.general.query_timeout(),
            client_idle_timeout: if admin {
                config.config.general.client_idle_timeout()
            } else {
                config.client_idle_timeout(user, database)
            },
            idle_in_transaction_timeout: config.config.general.client_idle_in_transaction_timeout(),
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
            State::IdleInTransaction => {
                // Client is sending the request, don't fire.
                if !client_request.messages.is_empty() {
                    Duration::MAX
                } else {
                    self.idle_in_transaction_timeout
                }
            }

            _ => Duration::MAX,
        }
    }
}

#[cfg(test)]
mod test {
    use crate::{config::config, net::Query};

    use super::*;

    #[test]
    fn test_idle_in_transaction_timeout() {
        let config = config(); // Will be default.
        let timeout = Timeouts::from_config(&config, "postgres", "postgres", false);

        let actual =
            timeout.client_idle_timeout(&State::IdleInTransaction, &ClientRequest::default());
        assert_eq!(actual, timeout.idle_in_transaction_timeout);
        assert_eq!(actual.as_millis(), i64::MAX as u128);

        let actual = timeout.client_idle_timeout(
            &State::IdleInTransaction,
            &ClientRequest::from(vec![Query::new("SELECT 1").into()]),
        );
        assert_eq!(actual, Duration::MAX);
    }

    #[test]
    fn from_config_uses_per_user_client_idle_timeout_override() {
        use pgdog_config::{Config, ConfigAndUsers, General, User, Users};

        let config = ConfigAndUsers {
            config: Config {
                general: General {
                    client_idle_timeout: 60_000,
                    ..Default::default()
                },
                ..Default::default()
            },
            users: Users {
                users: vec![User {
                    name: "listener".into(),
                    database: "pgdog".into(),
                    client_idle_timeout: Some(0),
                    ..Default::default()
                }],
                ..Default::default()
            },
            ..Default::default()
        };

        let timeouts = Timeouts::from_config(&config, "listener", "pgdog", false);
        assert_eq!(timeouts.client_idle_timeout, Duration::MAX);

        let timeouts = Timeouts::from_config(&config, "other", "pgdog", false);
        assert_eq!(timeouts.client_idle_timeout, Duration::from_millis(60_000));
    }

    #[test]
    fn authenticated_admin_keeps_general_timeout_after_admin_config_changes() {
        use pgdog_config::{Config, ConfigAndUsers, General, User, Users};

        let mut config = ConfigAndUsers {
            config: Config {
                general: General {
                    client_idle_timeout: 60_000,
                    ..Default::default()
                },
                ..Default::default()
            },
            users: Users {
                users: vec![User {
                    name: "admin".into(),
                    all_databases: true,
                    client_idle_timeout: Some(0),
                    ..Default::default()
                }],
                ..Default::default()
            },
            ..Default::default()
        };
        // Simulate a reload that changes the configured admin identity while
        // an existing admin session remains authenticated as admin/admin.
        config.config.admin.user = "root".into();
        config.config.admin.name = "control".into();

        let admin = Timeouts::from_config(&config, "admin", "admin", true);
        assert_eq!(admin.client_idle_timeout, Duration::from_millis(60_000));

        let regular = Timeouts::from_config(&config, "admin", "admin", false);
        assert_eq!(regular.client_idle_timeout, Duration::MAX);
    }
}
