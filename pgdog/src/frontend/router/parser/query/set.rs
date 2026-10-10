use super::*;
use pg_raw_parse::{Node, nodes, nodes::VariableSetKind::*};

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
        stmt: &nodes::VariableSetStmt,
        context: &QueryParserContext,
    ) -> Result<Command, Error> {
        if stmt.kind == VAR_RESET_ALL {
            Ok(Command::ResetAll)
        } else if stmt.kind == VAR_SET_MULTI || stmt.kind == VAR_SET_CURRENT {
            // SET TRANSACTION, or SET ... FROM CURRENT which doesn't change the value.
            Ok(Command::Query(
                Route::write(context.shards_calculator.shard().clone())
                    .with_read(context.read_only),
            ))
        } else {
            let param = Self::parse_set_param(stmt)?;
            Ok(Command::Set {
                params: vec![param],
                route: Route::write(context.shards_calculator.shard()).session_control(),
                set_config: false,
            })
        }
    }

    /// Parse a single SET statement into a SetParam
    fn parse_set_param(stmt: &nodes::VariableSetStmt) -> Result<SetParam, Error> {
        let value = if stmt.kind == VAR_SET_VALUE {
            Some(Self::parse_set_values(stmt)?)
        } else if stmt.kind == VAR_RESET || stmt.kind == VAR_SET_DEFAULT {
            None
        } else {
            return Err(Error::UnsupportedSetKind(stmt.kind));
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

    /// Try to handle multi-statement queries containing SET commands.
    ///
    /// - All SETs → returns `Ok(Some(Command::Set { .. }))`
    /// - No SETs → returns `Ok(None)`, caller falls through to default parsing
    /// - Mix of SET + non-SET, or RESET ALL → returns `Err(MultiStatementMixedSet)`
    ///   so the caller can split the statements and preserve their order.
    pub(super) fn try_multi_set<'a>(
        &self,
        stmts: impl IntoIterator<Item = &'a nodes::RawStmt>,
        context: &QueryParserContext,
    ) -> Result<Option<Command>, Error> {
        let mut has_other = false;
        let mut has_reset_all = false;

        let params = stmts
            .into_iter()
            .filter_map(|stmt| match stmt.stmt() {
                Node::VariableSetStmt(stmt)
                    if matches!(stmt.kind, VAR_SET_VALUE | VAR_SET_DEFAULT | VAR_RESET) =>
                {
                    Some(Self::parse_set_param(stmt))
                }
                Node::VariableSetStmt(stmt) if stmt.kind == VAR_RESET_ALL => {
                    has_reset_all = true;
                    None
                }
                _ => {
                    has_other = true;
                    None
                }
            })
            .collect::<Result<Vec<_>, _>>()?;

        if has_reset_all {
            Err(Error::MultiStatementMixedSet)
        } else if params.is_empty() {
            Ok(None)
        } else if has_other {
            Err(Error::MultiStatementMixedSet)
        } else {
            Ok(Some(Command::Set {
                params,
                route: Route::write(context.shards_calculator.shard()).session_control(),
                set_config: false,
            }))
        }
    }

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
}
