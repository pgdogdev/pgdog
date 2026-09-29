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

        // This reduces write latency as we don't have to wait for disk I/O.
        // If it was 'on', every commit would fsync after, making execution serial,
        // capping throughput. With no bottlenecks on our side, on the first WAL checkpoint,
        // throughput was observed in benchmarking to be significantly reduced.
        //
        // This is what CREATE SUBSCRIPTION does by default.
        //
        // Why is it safe? We query the database for pg_current_wal_insert_lsn and pg_current_wal_flush_lsn
        // for each shard to maintain an accurate view of Postgres's state, so that we know
        // when it's okay for the source to discard WAL; we won't go past the corresponding transaction LSN unless we
        // see that every destination shard's flush_lsn is at or has gone past it.
        // Thus, if the shard crashes, it's fine as the data is still on source db, and can be re-sent.
        //
        // See `StreamSubscriber::check_for_committed_transaction`
        options.add(Parameter {
            name: "synchronous_commit".into(),
            value: "off".into(),
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
