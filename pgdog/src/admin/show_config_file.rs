//! SHOW CONFIG FILE command.

use crate::config::config;

use super::prelude::*;

pub struct ShowConfigFile;

#[async_trait]
impl Command for ShowConfigFile {
    fn name(&self) -> String {
        "SHOW".into()
    }

    fn parse(_sql: &str) -> Result<Self, Error> {
        Ok(Self {})
    }

    async fn execute(&self) -> Result<Vec<Message>, Error> {
        let config = config();
        let snapshot = sanitize_for_toml(config.config.clone());
        let toml = toml::to_string_pretty(&snapshot)?;

        let mut messages = vec![RowDescription::new(&[Field::text("pgdog.toml")]).message()?];

        let mut row = DataRow::new();
        row.add(&toml);
        messages.push(row.message()?);

        Ok(messages)
    }
}

fn sanitize_for_toml(mut config: crate::config::Config) -> crate::config::Config {
    let max_i64 = i64::MAX as u64;
    let max_i64_usize = i64::MAX as usize;

    if config.general.client_idle_timeout > max_i64 {
        config.general.client_idle_timeout = max_i64;
    }

    if config.general.query_timeout > max_i64 {
        config.general.query_timeout = max_i64;
    }

    if config.general.lsn_check_delay > max_i64 {
        config.general.lsn_check_delay = max_i64;
    }

    if config.general.prepared_statements_limit > max_i64_usize {
        config.general.prepared_statements_limit = max_i64_usize;
    }

    config
}
