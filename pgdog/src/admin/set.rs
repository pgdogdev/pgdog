use crate::config::config;

use super::prelude::*;
use pg_query::{parse, protobuf::a_const, NodeEnum};

pub struct Set {
    name: String,
    value: u64,
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
                        Some(a_const::Val::Ival(val)) => {
                            return Ok(Self {
                                name,
                                value: val.ival as u64,
                            });
                        }

                        _ => return Err(Error::Syntax),
                    },

                    _ => return Err(Error::Syntax),
                }
            }

            _ => Err(Error::Syntax),
        }
    }

    async fn execute(&self) -> Result<Vec<Message>, Error> {
        let mut general = config().config.general.clone();
        match self.name.as_str() {
            "query_timeout" => {
                general.query_timeout = self.value;
            }

            "checkout_timeout" => {
                general.checkout_timeout = self.value;
            }

            _ => return Err(Error::Syntax),
        }

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
        assert_eq!(cmd.value, 5000);
    }
}
