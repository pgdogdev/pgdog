use itertools::Itertools;
use pg_raw_parse::{
    Node,
    nodes::{self, SelectStmt},
};

use super::{Error, StatementParameters};

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Hash)]
pub(crate) struct Limit {
    pub(crate) limit: Option<usize>,
    pub(crate) offset: Option<usize>,
}

#[derive(Debug, Clone)]
pub(crate) struct LimitClause<'a> {
    stmt: &'a SelectStmt,
    bind: Option<StatementParameters<'a>>,
}

impl<'a> LimitClause<'a> {
    pub(crate) fn new(stmt: &'a SelectStmt, bind: Option<StatementParameters<'a>>) -> Self {
        Self { stmt, bind }
    }

    pub(crate) fn limit_offset(&self) -> Result<Limit, Error> {
        Ok(Limit {
            limit: self.decode(self.stmt.limit_count())?,
            offset: self.decode(self.stmt.limit_offset())?,
        })
    }

    /// Whether PgDog can compute this LIMIT or OFFSET value: integer literals,
    /// parameters, casts to integer types, and `+`, `-`, `*` over them.
    pub(crate) fn is_computable(node: Node<'_>) -> bool {
        match node {
            Node::None | Node::ParamRef(_) => true,
            Node::A_Const(constant) => constant
                .val()
                .is_none_or(|value| value.numeric_value::<i64>().is_some()),
            Node::TypeCast(cast) => is_integer_cast(cast) && Self::is_computable(cast.arg()),
            Node::A_Expr(expr) => {
                operator(expr).is_some()
                    && Self::is_computable(expr.lexpr())
                    && Self::is_computable(expr.rexpr())
            }
            _ => false,
        }
    }

    /// Values PgDog can't compute are `None`, the same as no limit.
    // FIXME: We should error and not silently treat as NULL
    fn decode(&self, node: Node<'_>) -> Result<Option<usize>, Error> {
        if !Self::is_computable(node) {
            return Ok(None);
        }
        Ok(self
            .evaluate(node)?
            .and_then(|value| usize::try_from(value).ok()))
    }

    /// Compute a value accepted by [`Self::is_computable`]. `None` is no limit.
    ///
    /// Values Postgres rejects, e.g. a negative LIMIT, an overflow or an out of
    /// range cast, don't matter: every shard query still evaluates the original
    /// expression, so Postgres returns its own error to the client.
    fn evaluate(&self, node: Node<'_>) -> Result<Option<i64>, Error> {
        Ok(match node {
            Node::A_Const(constant) => constant.val().and_then(|value| value.numeric_value()),
            Node::ParamRef(param) => self.parameter(param.number as usize)?,
            Node::TypeCast(cast) => self.evaluate(cast.arg())?,
            Node::A_Expr(expr) => {
                let left = self.evaluate(expr.lexpr())?;
                let right = self.evaluate(expr.rexpr())?;
                left.zip(right)
                    .zip(operator(expr))
                    .and_then(|((left, right), apply)| apply(left, right))
            }
            _ => None,
        })
    }

    fn parameter(&self, number: usize) -> Result<Option<i64>, Error> {
        let Some(bind) = self.bind else {
            // FIXME: This is a Parse with no Bind. We don't need to
            // be rewriting the query at all if we have no Bind, and
            // should be able to treat this as an error condition
            return Ok(None);
        };
        let param = bind
            .parameter(number - 1)?
            .ok_or_else(|| Error::MissingParameter(number))?;

        if param.is_null() {
            Ok(None)
        } else {
            match param.bigint() {
                Some(param) => Ok(Some(param)),
                None => Err(Error::ParameterNotInteger(number, param.text_debug())),
            }
        }
    }
}

/// The binary `+`, `-` or `*` operator of this expression. Other operators
/// are left out: `$1 / 2` is numeric division if the client declares `$1`
/// as numeric.
fn operator(expr: &nodes::A_Expr) -> Option<fn(i64, i64) -> Option<i64>> {
    if expr.kind != nodes::A_Expr_Kind::AEXPR_OP || matches!(expr.lexpr(), Node::None) {
        return None;
    }
    let apply: fn(i64, i64) -> Option<i64> =
        match expr.name().into_iter().exactly_one().ok()?.as_str()? {
            "+" => i64::checked_add,
            "-" => i64::checked_sub,
            "*" => i64::checked_mul,
            _ => return None,
        };
    Some(apply)
}

/// Whether this is `x::int2`, `x::int4` or `x::int8`, or one of their aliases.
fn is_integer_cast(cast: &nodes::TypeCast) -> bool {
    let Some(type_name) = cast.type_name() else {
        return false;
    };
    if !type_name.array_bounds().is_empty() {
        return false;
    }
    let mut names = type_name.names().iter().map(|name| name.sval());
    matches!(
        (names.next(), names.next(), names.next()),
        (Some(Some("int2" | "int4" | "int8")), None, _)
            | (
                Some(Some("pg_catalog")),
                Some(Some("int2" | "int4" | "int8")),
                None
            )
    )
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::net::messages::{Format, bind::Bind, bind::Parameter};
    use bytes::Bytes;

    fn limit(sql: &str, bind: Option<&Bind>) -> Result<Limit, Error> {
        let ast = pg_raw_parse::parse(sql).unwrap();
        let Some(Node::SelectStmt(stmt)) = ast.stmts().next() else {
            panic!("expected SELECT");
        };
        LimitClause::new(stmt, bind.map(StatementParameters::from)).limit_offset()
    }

    fn limit_count(sql: &str) -> Option<usize> {
        limit(sql, None).unwrap().limit
    }

    fn computable(sql: &str) -> bool {
        let ast = pg_raw_parse::parse(sql).unwrap();
        let Some(Node::SelectStmt(stmt)) = ast.stmts().next() else {
            panic!("expected SELECT");
        };
        LimitClause::is_computable(stmt.limit_count())
    }

    #[test]
    fn evaluates_integer_arithmetic() {
        for (sql, expected) in [
            ("SELECT 1 LIMIT 1 + 1", Some(2)),
            ("SELECT 1 LIMIT 10 - 3 * 2", Some(4)),
            ("SELECT 1 LIMIT (2 + 3) * 2", Some(10)),
            ("SELECT 1 LIMIT 10000000000", Some(10_000_000_000)),
            ("SELECT 1 LIMIT 5::int", Some(5)),
            ("SELECT 1 LIMIT CAST(5 AS bigint)", Some(5)),
            ("SELECT 1 LIMIT 5::int4 + 1::int8", Some(6)),
            ("SELECT 1 LIMIT 1 + NULL", None),
            ("SELECT 1 LIMIT ALL", None),
            // Postgres raises these errors on the shards.
            ("SELECT 1 LIMIT -1", None),
            ("SELECT 1 LIMIT 9223372036854775807 + 1", None),
        ] {
            assert_eq!(limit_count(sql), expected, "{sql}");
        }
    }

    #[test]
    fn evaluates_parameters() {
        let text = Bind::new_params("", &[Parameter::new(b"5"), Parameter::new(b"2")]);
        let text = limit("SELECT 1 LIMIT $1::int + 1 OFFSET $2 * 2", Some(&text)).unwrap();
        assert_eq!(text.limit, Some(6));
        assert_eq!(text.offset, Some(4));

        // Drivers send `$1::int` as a 4-byte binary integer.
        let binary = Bind::new_params_codes(
            "",
            &[Parameter {
                len: 4,
                data: Bytes::copy_from_slice(&7i32.to_be_bytes()),
            }],
            &[Format::Binary],
        );
        assert_eq!(
            limit("SELECT 1 LIMIT $1::int", Some(&binary))
                .unwrap()
                .limit,
            Some(7)
        );

        let fractional = Bind::new_params("", &[Parameter::new(b"2.5")]);
        assert!(matches!(
            limit("SELECT 1 LIMIT $1::int", Some(&fractional)),
            Err(Error::ParameterNotInteger(1, _))
        ));
        // Postgres types `$1` as numeric here, so PgDog doesn't read it.
        for sql in ["SELECT 1 LIMIT $1::numeric", "SELECT 1 LIMIT $1 + 0.5"] {
            assert_eq!(limit(sql, Some(&fractional)).unwrap().limit, None, "{sql}");
        }
    }

    #[test]
    fn rejects_values_pgdog_cannot_compute() {
        for sql in [
            "SELECT 1 LIMIT (SELECT 1)",
            "SELECT 1 LIMIT random()::int",
            "SELECT 1 LIMIT '1'",
            "SELECT 1 LIMIT 2.5",
            "SELECT 1 LIMIT 5::numeric",
            "SELECT 1 LIMIT 5::int[]",
            "SELECT 1 LIMIT 10 / 2",
            // The parser folds `-(2)` into the constant -2, so use a parameter.
            "SELECT 1 LIMIT -$1",
            "SELECT 1 LIMIT 5 OPERATOR(pg_catalog.+) 1",
        ] {
            assert!(!computable(sql), "{sql}");
        }
        for sql in ["SELECT 1 LIMIT $1 + 1", "SELECT 1 LIMIT $1::bigint * $2"] {
            assert!(computable(sql), "{sql}");
        }
    }
}
