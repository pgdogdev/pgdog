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
        } else if stmt.kind == VAR_SET_MULTI {
            // SET TRANSACTION
            Ok(Command::Query(
                Route::write(context.shards_calculator.shard().clone())
                    .with_read(context.read_only),
            ))
        } else {
            let param = Self::parse_set_param(stmt, context.query()?.query())?;
            Ok(Command::Set {
                params: vec![param],
                route: Route::write(context.shards_calculator.shard()),
                set_config: false,
            })
        }
    }

    /// Parse a single SET statement into a SetParam
    fn parse_set_param(stmt: &nodes::VariableSetStmt, query: &str) -> Result<SetParam, Error> {
        let mut value = if stmt.kind == VAR_SET_VALUE {
            Some(Self::parse_set_values(stmt)?)
        } else if stmt.kind == VAR_RESET || stmt.kind == VAR_SET_DEFAULT {
            None
        } else {
            panic!("parse_set_param called on invalid kind {}", stmt.kind);
        };
        let name = stmt.name().expect("SET always has name");

        // PostgreSQL's raw parse tree normalizes both NONE and a quoted role
        // named "none" to the same string. Preserve the keyword form by
        // checking the original token at its parser-provided location.
        if name == "role"
            && value.as_ref().and_then(ParameterValue::as_str) == Some("none")
            && let Some(Node::A_Const(constant)) = stmt.args().first()
            && let Some(first) = query.as_bytes().get(constant.location as usize)
            && !matches!(first, b'\'' | b'"')
        {
            value = None;
        }

        match value {
            value @ Some(_) => Ok(SetParam {
                name: name.to_string(),
                value,
                local: stmt.is_local,
            }),
            None => Ok(SetParam {
                name: name.to_string(),
                value: None,
                local: stmt.is_local,
            }),
        }
    }

    /// Try to handle multi-statement queries containing SET commands.
    ///
    /// - All SETs → returns `Ok(Some(Command::Set { .. }))`
    /// - No SETs → returns `Ok(None)`, caller falls through to default parsing
    /// - Mix of SET + non-SET → returns `Err(MultiStatementMixedSet)`
    ///
    /// In session mode, returns `Ok(Some(Command::Query(..)))` immediately so that
    /// all multi-statement queries are forwarded to the server verbatim.
    pub(super) fn try_multi_set<'a>(
        &self,
        stmts: impl IntoIterator<Item = &'a nodes::RawStmt>,
        context: &QueryParserContext,
    ) -> Result<Option<Command>, Error> {
        let mut has_other = false;
        let query = context.query()?.query();

        let params = stmts
            .into_iter()
            .filter_map(|stmt| match stmt.stmt() {
                Node::VariableSetStmt(stmt) if stmt.kind != VAR_SET_MULTI => {
                    Some(Self::parse_set_param(stmt, query))
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
