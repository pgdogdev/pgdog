use crate::{
    backend::databases,
    config::{self, config},
};

use super::prelude::*;
use pg_query::{parse, protobuf::a_const, NodeEnum};

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
                config.config.general.auth_type =
                    serde_json::from_str(&format!(r#""{}""#, self.value))?;
            }

            "read_write_strategy" => {
                config.config.general.read_write_strategy =
                    serde_json::from_str(&format!(r#""{}""#, self.value))?;
            }

            _ => return Err(Error::Syntax),
        }

        config::set(config)?;
        databases::init();

        Ok(vec![])
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
