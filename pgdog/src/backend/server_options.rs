use std::time::Duration;

use super::pool::Config;

use crate::net::{Parameter, parameter::ParameterValue};

#[derive(Debug, Clone)]
pub(crate) struct ServerOptions {
    pub(crate) params: Vec<Parameter>,
    pub(crate) session_replication_role: bool,
}

impl Default for ServerOptions {
    fn default() -> Self {
        Self {
            session_replication_role: false,
            params: vec![
                Parameter {
                    name: "application_name".into(),
                    value: "PgDog".into(),
                },
                Parameter {
                    name: "client_encoding".into(),
                    value: "utf-8".into(),
                },
            ],
        }
    }
}

impl ServerOptions {
    pub(crate) fn add(&mut self, parameter: Parameter) {
        self.params.push(parameter);
    }

    pub(crate) fn replication_mode(&self) -> bool {
        self.params.iter().any(|p| {
            p.name == "replication"
                && match p.value {
                    ParameterValue::String(ref value) => value == "database",
                    _ => false,
                }
        })
    }

    pub(crate) fn new_replication() -> Self {
        let mut options = Self::default();
        options.add(Parameter {
            name: "replication".into(),
            value: "database".into(),
        });
        options
    }

    pub(crate) fn new_resharding(config: &Config) -> Self {
        let mut options = Self {
            // This can't be set via startup parameters for some mysterious reason.
            session_replication_role: true,
            ..Default::default()
        };

        options.add(Parameter {
            name: "statement_timeout".into(),
            value: "0".into(),
        });
        // Enforce some lock_timeout during resharding to prevent possible deadlocks.
        // This should be mostly avoided by pgdog, but in case some invariants are not met,
        // the resharding could deadlock and with timeout we'll probably retry the update
        // and either succeed or fail explicitly.
        options.add(Parameter {
            name: "lock_timeout".into(),
            value: config
                .lock_timeout
                .unwrap_or(Duration::from_secs(5))
                .as_millis()
                .to_string()
                .into(),
        });
        options
    }
}
