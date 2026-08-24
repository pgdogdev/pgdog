use pgdog_config::General;

use super::timeouts::Timeouts;

/// Per-request snapshot of client settings read from config.
///
/// Filled once in [`super::Client::buffer`] and passed through the query engine
/// so mid-request config reloads don't change behavior.
#[derive(Debug, Clone, Copy)]
pub(crate) struct ClientRequestSettings {
    pub(crate) timeouts: Timeouts,
    pub(crate) query_log_stdout: bool,
    pub(crate) query_size_limit: Option<usize>,
    pub(crate) application_name_add_host: bool,
    pub(crate) expanded_explain: bool,
    pub(crate) log_query_sample_length: usize,
    pub(crate) frontend_query_size_limit_block: Option<usize>,
}

impl Default for ClientRequestSettings {
    fn default() -> Self {
        Self {
            timeouts: Timeouts::default(),
            query_log_stdout: false,
            query_size_limit: None,
            application_name_add_host: false,
            expanded_explain: false,
            log_query_sample_length: General::log_query_sample_length(),
            frontend_query_size_limit_block: None,
        }
    }
}

impl ClientRequestSettings {
    pub(crate) fn from_general(general: &General) -> Self {
        Self {
            timeouts: Timeouts::from_config(general),
            query_log_stdout: general.query_log_stdout,
            query_size_limit: general.query_size_limit,
            application_name_add_host: general.application_name_add_host,
            expanded_explain: general.expanded_explain,
            log_query_sample_length: general.log_query_sample_length,
            frontend_query_size_limit_block: general.frontend_query_size_limit_block(),
        }
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use pgdog_config::QuerySizeLimitAction;

    use crate::{config::General, frontend::ClientRequest, state::State};

    use super::*;

    #[test]
    fn default_settings_match_safe_defaults() {
        let settings = ClientRequestSettings::default();

        assert!(!settings.query_log_stdout);
        assert_eq!(settings.query_size_limit, None);
        assert!(!settings.application_name_add_host);
        assert!(!settings.expanded_explain);
        assert_eq!(
            settings.log_query_sample_length,
            General::log_query_sample_length()
        );
        assert_eq!(settings.frontend_query_size_limit_block, None);
        assert_eq!(
            settings.timeouts.query_timeout(&State::Active),
            Duration::MAX
        );
        assert_eq!(
            settings
                .timeouts
                .client_idle_timeout(&State::Idle, &ClientRequest::default()),
            Duration::MAX
        );
        assert_eq!(
            settings
                .timeouts
                .client_idle_timeout(&State::IdleInTransaction, &ClientRequest::default()),
            Duration::MAX
        );
    }

    #[test]
    fn from_general_copies_all_snapshotted_fields() {
        let mut general = General::default();
        general.query_timeout = 1_000;
        general.client_idle_timeout = 2_000;
        general.client_idle_in_transaction_timeout = 3_000;
        general.query_log_stdout = true;
        general.query_size_limit = Some(4096);
        general.query_size_limit_action = QuerySizeLimitAction::Block;
        general.application_name_add_host = true;
        general.expanded_explain = true;
        general.log_query_sample_length = 42;

        let settings = ClientRequestSettings::from_general(&general);

        assert!(settings.query_log_stdout);
        assert_eq!(settings.query_size_limit, Some(4096));
        assert!(settings.application_name_add_host);
        assert!(settings.expanded_explain);
        assert_eq!(settings.log_query_sample_length, 42);
        assert_eq!(settings.frontend_query_size_limit_block, Some(4096));
        assert_eq!(
            settings.timeouts.query_timeout(&State::Active),
            Duration::from_millis(1_000)
        );
        assert_eq!(
            settings
                .timeouts
                .client_idle_timeout(&State::Idle, &ClientRequest::default()),
            Duration::from_millis(2_000)
        );
        assert_eq!(
            settings
                .timeouts
                .client_idle_timeout(&State::IdleInTransaction, &ClientRequest::default()),
            Duration::from_millis(3_000)
        );
    }

    #[test]
    fn from_general_omits_block_limit_when_action_is_warn() {
        let mut general = General::default();
        general.query_size_limit = Some(1024);
        general.query_size_limit_action = QuerySizeLimitAction::Warn;

        let settings = ClientRequestSettings::from_general(&general);

        assert_eq!(settings.query_size_limit, Some(1024));
        assert_eq!(settings.frontend_query_size_limit_block, None);
    }
}
