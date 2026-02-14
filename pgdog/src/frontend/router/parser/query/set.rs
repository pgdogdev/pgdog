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
        if let Some(param) = Self::parse_set_param(stmt)? {
            return Ok(Command::Set {
                params: vec![param],
                route: Route::write(context.shards_calculator.shard()),
            });
        }

        Ok(Command::Query(
            Route::write(context.shards_calculator.shard().clone()).with_read(context.read_only),
        ))
    }

    /// Parse a single SET statement into a SetParam, if it's not a transaction-level SET.
    pub fn parse_set_param(stmt: &VariableSetStmt) -> Result<Option<SetParam>, Error> {
        let transaction_state = stmt.name.starts_with("TRANSACTION");
        let value = Self::parse_set_value(stmt)?;

        match value {
            Some(value) if !transaction_state => Ok(Some(SetParam {
                name: stmt.name.to_string(),
                value,
                local: stmt.is_local,
            })),
            _ => Ok(None),
        }
    }

    /// Try to handle multi-statement queries containing SET commands.
    ///
    /// - All SETs → returns `Ok(Some(Command::Set { .. }))`
    /// - No SETs → returns `Ok(None)`, caller falls through to default parsing
    /// - Mix of SET + non-SET → returns `Err(MultiStatementMixedSet)`
    pub(super) fn try_multi_set(
        &mut self,
        stmts: &[RawStmt],
        context: &QueryParserContext,
    ) -> Result<Option<Command>, Error> {
        let mut params = Vec::with_capacity(stmts.len());
        let mut has_set = false;
        let mut has_other = false;

        for raw_stmt in stmts {
            let node = raw_stmt
                .stmt
                .as_ref()
                .and_then(|s| s.node.as_ref())
                .ok_or(Error::EmptyQuery)?;

            match node {
                NodeEnum::VariableSetStmt(stmt) => {
                    if stmt.name.starts_with("TRANSACTION") {
                        has_other = true;
                    } else {
                        has_set = true;
                        if let Some(param) = Self::parse_set_param(stmt)? {
                            params.push(param);
                        }
                    }
                }
                _ => has_other = true,
            }

            if has_set && has_other {
                return Err(Error::MultiStatementMixedSet);
            }
        }

        if params.is_empty() {
            return Ok(None);
        }

        Ok(Some(Command::Set {
            params,
            route: Route::write(context.shards_calculator.shard()),
        }))
    }

    pub fn parse_set_value(stmt: &VariableSetStmt) -> Result<Option<ParameterValue>, Error> {
        let mut value = vec![];

        for node in &stmt.args {
            match &node.node {
                Some(NodeEnum::AConst(AConst { val: Some(val), .. })) => match val {
                    Val::Sval(String { sval }) => value.push(sval.to_string()),
                    Val::Ival(Integer { ival }) => value.push(ival.to_string()),
                    Val::Fval(Float { fval }) => value.push(fval.to_string()),
                    Val::Boolval(Boolean { boolval }) => value.push(boolval.to_string()),
                    _ => (),
                },
                // e.g. SET TIME ZONE INTERVAL '+00:00' HOUR TO MINUTE
                Some(NodeEnum::TypeCast(tc)) => {
                    if let Some(ref arg) = tc.arg {
                        if let Some(NodeEnum::AConst(AConst {
                            val: Some(Val::Sval(String { ref sval })),
                            ..
                        })) = arg.node
                        {
                            value.push(sval.to_string());
                        }
                    }
                }
                _ => (),
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
