use super::*;

impl QueryParser {
    /// Handle the SET command.
    ///
    /// We allow setting shard/sharding key manually outside
    /// the normal protocol flow. This command is not forwarded to the server.
    ///
    /// All other SETs change the params on the client and are eventually sent to the server
    /// when the client is connected to the server.
    pub(super) fn set(
        &mut self,
        stmt: &VariableSetStmt,
        context: &QueryParserContext,
    ) -> Result<Command, Error> {
        let transaction_state = stmt.name.starts_with("TRANSACTION");
        let value = Self::parse_set_value(stmt)?;

        if let Some(value) = value {
            if !transaction_state {
                return Ok(Command::Set {
                    name: stmt.name.to_string(),
                    value,
                    local: stmt.is_local,
                    route: Route::write(context.shards_calculator.shard()),
                });
            }
        }

        Ok(Command::Query(
            Route::write(context.shards_calculator.shard().clone()).with_read(context.read_only),
        ))
    }

    pub fn parse_set_value(stmt: &VariableSetStmt) -> Result<Option<ParameterValue>, Error> {
        let mut value = vec![];

        for node in &stmt.args {
            if let Some(NodeEnum::AConst(AConst { val: Some(val), .. })) = &node.node {
                match val {
                    Val::Sval(String { sval }) => {
                        value.push(sval.to_string());
                    }

                    Val::Ival(Integer { ival }) => {
                        value.push(ival.to_string());
                    }

                    Val::Fval(Float { fval }) => {
                        value.push(fval.to_string());
                    }

                    Val::Boolval(Boolean { boolval }) => {
                        value.push(boolval.to_string());
                    }

                    _ => (),
                }
            }
        }

        let value = match value.len() {
            0 => None,
            1 => Some(ParameterValue::String(value.pop().unwrap())),
            _ => Some(ParameterValue::Tuple(value)),
        };

        Ok(value)
    }
}
