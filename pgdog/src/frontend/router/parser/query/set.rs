use super::*;
#[cfg(feature = "new_parser")]
use pg_raw_parse::{Node, nodes, nodes::VariableSetKind::*};

impl QueryParser {
    /// Handle the SET command.
    ///
    /// We allow setting shard/sharding key manually outside
    /// the normal protocol flow. This command is not forwarded to the server.
    ///
    /// All other SETs change the params on the client and are eventually sent to the server
    /// when the client is connected to the server.
    #[cfg(feature = "new_parser")]
    pub(super) fn set(
        &mut self,
        stmt: &nodes::VariableSetStmt,
        context: &QueryParserContext,
    ) -> Result<Command, Error> {
        if stmt.kind == VAR_RESET_ALL {
            Ok(Command::ResetAll)
        } else if stmt.kind == VAR_SET_MULTI {
            // SET TRANSACTION
            Ok(Command::Query(
                Route::write(context.shards_calculator.shard().clone())
                    .with_read(context.read_only),
            ))
        } else {
            let param = Self::parse_set_param(stmt)?;
            Ok(Command::Set {
                params: vec![param],
                route: Route::write(context.shards_calculator.shard()),
                behave_like_select: false,
            })
        }
    }

    cfg_select! {
        not(feature = "new_parser") => {
            pub(super) fn set(
                &mut self,
                stmt: &VariableSetStmt,
                context: &QueryParserContext,
            ) -> Result<Command, Error> {
                if stmt.kind() == VariableSetKind::VarResetAll {
                    return Ok(Command::ResetAll);
                }

                if let Some(param) = Self::parse_set_param(stmt)? {
                    return Ok(Command::Set {
                        params: vec![param],
                        route: Route::write(context.shards_calculator.shard()),
                        behave_like_select: false,
                    });
                }

                Ok(Command::Query(
                    Route::write(context.shards_calculator.shard().clone()).with_read(context.read_only),
                ))
            }
        }
        _ => {}
    }

    /// Parse a single SET statement into a SetParam
    #[cfg(feature = "new_parser")]
    fn parse_set_param(stmt: &nodes::VariableSetStmt) -> Result<SetParam, Error> {
        let value = if stmt.kind == VAR_SET_VALUE {
            Some(Self::parse_set_values(stmt)?)
        } else if stmt.kind == VAR_RESET || stmt.kind == VAR_SET_DEFAULT {
            None
        } else {
            panic!("parse_set_param called on invalid kind {}", stmt.kind);
        };

        match value {
            value @ Some(_) => Ok(SetParam {
                name: stmt.name().expect("SET always has name").to_string(),
                value,
                local: stmt.is_local,
            }),
            None => Ok(SetParam {
                name: stmt.name().expect("SET always has name").to_string(),
                value: None,
                local: false,
            }),
        }
    }

    cfg_select! {
        not(feature = "new_parser") => {
            fn parse_set_param(stmt: &VariableSetStmt) -> Result<Option<SetParam>, Error> {
                let is_reset = stmt.kind() == VariableSetKind::VarReset;
                let value = Self::parse_set_value(stmt)?;

                match value {
                    value @ Some(_) => Ok(Some(SetParam {
                        name: stmt.name.to_string(),
                        value,
                        local: stmt.is_local,
                    })),
                    None if is_reset => Ok(Some(SetParam {
                        name: stmt.name.to_string(),
                        value: None,
                        local: false,
                    })),
                    None => Ok(None),
                }
            }
        }
        _ => {}
    }

    /// Try to handle multi-statement queries containing SET commands.
    ///
    /// - All SETs → returns `Ok(Some(Command::Set { .. }))`
    /// - No SETs → returns `Ok(None)`, caller falls through to default parsing
    /// - Mix of SET + non-SET → returns `Err(MultiStatementMixedSet)`
    ///
    /// In session mode, returns `Ok(Some(Command::Query(..)))` immediately so that
    /// all multi-statement queries are forwarded to the server verbatim.
    #[cfg(feature = "new_parser")]
    pub(super) fn try_multi_set<'a>(
        &mut self,
        stmts: impl IntoIterator<Item = &'a nodes::RawStmt>,
        context: &QueryParserContext,
    ) -> Result<Option<Command>, Error> {
        // In session mode, pass through without validation — the server
        // owns the session and can handle mixed SET + other statements.
        if context.is_session_mode() {
            return Ok(Some(Command::Query(Route::write(
                context.shards_calculator.shard(),
            ))));
        }
        let mut has_other = false;

        let params = stmts
            .into_iter()
            .filter_map(|stmt| match stmt.stmt() {
                Node::VariableSetStmt(stmt) if stmt.kind != VAR_SET_MULTI => {
                    Some(Self::parse_set_param(stmt))
                }
                _ => {
                    has_other = true;
                    None
                }
            })
            .collect::<Result<Vec<_>, _>>()?;

        if params.is_empty() {
            Ok(None)
        } else if has_other {
            Err(Error::MultiStatementMixedSet)
        } else {
            Ok(Some(Command::Set {
                params,
                route: Route::write(context.shards_calculator.shard()),
                behave_like_select: false,
            }))
        }
    }

    cfg_select! {
        not(feature = "new_parser") => {
            pub(super) fn try_multi_set(
                &mut self,
                stmts: &[RawStmt],
                context: &QueryParserContext,
            ) -> Result<Option<Command>, Error> {
                // In session mode, pass through without validation — the server
                // owns the session and can handle mixed SET + other statements.
                if context.is_session_mode() {
                    return Ok(Some(Command::Query(Route::write(
                        context.shards_calculator.shard(),
                    ))));
                }
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
                    behave_like_select: false,
                }))
            }
        }
        _ => {}
    }

    #[cfg(feature = "new_parser")]
    fn parse_set_values(stmt: &nodes::VariableSetStmt) -> Result<ParameterValue, Error> {
        let mut value = stmt
            .args()
            .iter()
            .map(|node| match node {
                Node::A_Const(a) => Ok(a
                    .val()
                    .expect("SET value TO NULL is a parse error")
                    .to_string()),
                // e.g. SET TIME ZONE INTERVAL '+00:00' HOUR TO MINUTE
                Node::TypeCast(tc) if let Node::A_Const(a) = tc.arg() => Ok(a
                    .val()
                    .expect("SET value TO NULL is a parse error")
                    .to_string()),
                _ => Err(Error::ColumnDecode),
            })
            .collect::<Result<Vec<_>, _>>()?;

        let value = match value.len() {
            0 => panic!("parse_set_values called on RESET or SET TRANSACTION"),
            1 => ParameterValue::String(value.pop().unwrap()),
            _ => ParameterValue::Tuple(value),
        };

        Ok(value)
    }

    cfg_select! {
        not(feature = "new_parser") => {
            fn parse_set_value(stmt: &VariableSetStmt) -> Result<Option<ParameterValue>, Error> {
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
                            if let Some(ref arg) = tc.arg
                                && let Some(NodeEnum::AConst(AConst {
                                    val: Some(Val::Sval(String { ref sval })),
                                    ..
                                })) = arg.node
                            {
                                value.push(sval.to_string());
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
        _ => {}
    }
}
