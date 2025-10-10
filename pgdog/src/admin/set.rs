use crate::{
    backend::databases,
    config::{self, config, general::ShardKeyUpdateMode},
    frontend::PreparedStatements,
};

use super::prelude::*;
use pg_query::{parse, protobuf::a_const, NodeEnum};
use serde::de::DeserializeOwned;

pub struct Set {
    name: String,
    value: String,
}

#[async_trait]
impl Command for Set {
    fn name(&self) -> String {
        "SET".into()
    }

    fn parse(sql: &str) -> Result<Self, Error> {
        let stmt = parse(sql).map_err(|_| Error::Syntax)?;
        let root = stmt.protobuf.stmts.first().cloned().ok_or(Error::Syntax)?;
        let stmt = root.stmt.ok_or(Error::Syntax)?;
        match stmt.node.ok_or(Error::Syntax)? {
            NodeEnum::VariableSetStmt(stmt) => {
                let name = stmt.name;

                let setting = stmt.args.first().ok_or(Error::Syntax)?;
                let node = setting.node.clone().ok_or(Error::Syntax)?;
                match node {
                    NodeEnum::AConst(a_const) => match a_const.val {
                        Some(a_const::Val::Ival(val)) => Ok(Self {
                            name,
                            value: val.ival.to_string(),
                        }),

                        Some(a_const::Val::Sval(sval)) => Ok(Self {
                            name,
                            value: sval.sval.to_string(),
                        }),

                        _ => Err(Error::Syntax),
                    },

                    _ => Err(Error::Syntax),
                }
            }

            _ => Err(Error::Syntax),
        }
    }

    async fn execute(&self) -> Result<Vec<Message>, Error> {
        let _lock = databases::lock();
        let mut config = (*config()).clone();
        match self.name.as_str() {
            "query_timeout" => {
                config.config.general.query_timeout = self.value.parse()?;
            }

            "checkout_timeout" => {
                config.config.general.checkout_timeout = self.value.parse()?;
            }

            "auth_type" => {
                config.config.general.auth_type = Self::from_json(&self.value)?;
            }

            "passthrough_auth" => {
                config.config.general.passthrough_auth = Self::from_json(&self.value)?;
            }

            "read_write_strategy" => {
                config.config.general.read_write_strategy = Self::from_json(&self.value)?;
            }

            "read_write_split" => {
                config.config.general.read_write_split = Self::from_json(&self.value)?;
            }

            "load_balancing_strategy" => {
                config.config.general.load_balancing_strategy = Self::from_json(&self.value)?;
            }

            "prepared_statements_limit" => {
                config.config.general.prepared_statements_limit = self.value.parse()?;
                PreparedStatements::global()
                    .write()
                    .close_unused(config.config.general.prepared_statements_limit);
            }

            "cross_shard_disabled" => {
                config.config.general.cross_shard_disabled = Self::from_json(&self.value)?;
            }

            "two_phase_commit" => {
                config.config.general.two_phase_commit = Self::from_json(&self.value)?;
            }

            "two_phase_commit_auto" => {
                config.config.general.two_phase_commit_auto = Self::from_json(&self.value)?;
            }

            "rewrite_shard_key_updates" => {
                config.config.general.rewrite_shard_key_updates = self
                    .value
                    .parse::<ShardKeyUpdateMode>()
                    .map_err(|_| Error::Syntax)?;
            }

            "healthcheck_interval" => {
                config.config.general.healthcheck_interval = self.value.parse()?;
            }

            "idle_healthcheck_interval" => {
                config.config.general.idle_healthcheck_interval = self.value.parse()?;
            }

            "idle_healthcheck_delay" => {
                config.config.general.idle_healthcheck_delay = self.value.parse()?;
            }

            "ban_timeout" => {
                config.config.general.ban_timeout = self.value.parse()?;
            }

            _ => return Err(Error::Syntax),
        }

        config::set(config)?;
        databases::init();

        Ok(vec![])
    }
}

impl Set {
    fn from_json<T: DeserializeOwned>(value: &str) -> serde_json::Result<T> {
        let value = match value {
            "true" | "false" => value.to_string(),
            _ => format!(r#""{}""#, value),
        };
        serde_json::from_str::<T>(&value)
    }
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn test_set_command() {
        let cmd = "SET query_timeout TO 5000";
        let cmd = Set::parse(cmd).unwrap();
        assert_eq!(cmd.name, "query_timeout");
        assert_eq!(cmd.value, "5000");
    }
}
