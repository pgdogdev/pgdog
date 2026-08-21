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
            let param = Self::parse_set_param(stmt)?;
            Ok(Command::Set {
                params: vec![param],
                route: Route::write(context.shards_calculator.shard()),
                behave_like_select: false,
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
