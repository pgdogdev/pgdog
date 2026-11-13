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
    /// parameters, casts to integer types, and arithmetic over them.
    pub(crate) fn is_computable(node: Node<'_>) -> bool {
        match node {
            Node::None | Node::ParamRef(_) => true,
            Node::A_Const(constant) => constant
                .val()
                .is_none_or(|value| value.numeric_value::<i64>().is_some()),
            Node::TypeCast(cast) => {
                IntegerType::of(cast).is_some() && Self::is_computable(cast.arg())
            }
            Node::A_Expr(expr) => Operator::of(expr).is_some_and(|operator| {
                operands(expr).all(Self::is_computable)
                    // `$1 / 2` is numeric division if the client declares `$1` as numeric.
                    && (operator != Operator::Div || operands(expr).all(is_integer_typed))
            }),
            _ => false,
        }
    }

    fn decode(&self, node: Node<'_>) -> Result<Option<usize>, Error> {
        if !Self::is_computable(node) {
            return Ok(None); // FIXME: We should error and not silently treat as NULL
        }
        Ok(self
            .evaluate(node)?
            .and_then(|value| usize::try_from(value).ok()))
    }

    /// Compute a value accepted by [`Self::is_computable`]. `None` is no limit.
    ///
    /// Values Postgres rejects, e.g. an overflow or an out of range cast, are
    /// also `None`: every shard query still evaluates the original expression,
    /// so Postgres returns its own error to the client.
    fn evaluate(&self, node: Node<'_>) -> Result<Option<i64>, Error> {
        Ok(match node {
            Node::A_Const(constant) => constant.val().and_then(|value| value.numeric_value()),
            Node::ParamRef(param) => self.parameter(param.number as usize)?,
            Node::TypeCast(cast) => {
                let value = self.evaluate(cast.arg())?;
                IntegerType::of(cast).and_then(|integer| value.filter(|&value| integer.fits(value)))
            }
            Node::A_Expr(expr) => {
                let Some(operator) = Operator::of(expr) else {
                    return Ok(None);
                };
                let right = self.evaluate(expr.rexpr())?;
                match operator {
                    Operator::Negate => right.and_then(i64::checked_neg),
                    Operator::Identity => right,
                    _ => self
                        .evaluate(expr.lexpr())?
                        .zip(right)
                        .and_then(|(left, right)| operator.apply(left, right)),
                }
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

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Operator {
    Negate,
    Identity,
    Add,
    Subtract,
    Multiply,
    Div,
    Modulo,
}

impl Operator {
    fn of(expr: &nodes::A_Expr) -> Option<Self> {
        if expr.kind != nodes::A_Expr_Kind::AEXPR_OP {
            return None;
        }
        let name = expr.name().into_iter().exactly_one().ok()?.as_str()?;
        let unary = matches!(expr.lexpr(), Node::None);
        Some(match (name, unary) {
            ("-", true) => Self::Negate,
            ("+", true) => Self::Identity,
            ("+", false) => Self::Add,
            ("-", false) => Self::Subtract,
            ("*", false) => Self::Multiply,
            ("/", false) => Self::Div,
            ("%", false) => Self::Modulo,
            _ => return None,
        })
    }

    /// Integer arithmetic as Postgres does it: division truncates toward zero.
    fn apply(self, left: i64, right: i64) -> Option<i64> {
        match self {
            Self::Add => left.checked_add(right),
            Self::Subtract => left.checked_sub(right),
            Self::Multiply => left.checked_mul(right),
            Self::Div => left.checked_div(right),
            // Postgres returns 0 for `i64::MIN % -1` instead of overflowing.
            Self::Modulo if right == -1 => Some(0),
            Self::Modulo => left.checked_rem(right),
            Self::Negate | Self::Identity => None,
        }
    }
}

#[derive(Debug, Clone, Copy)]
enum IntegerType {
    Int2,
    Int4,
    Int8,
}

impl IntegerType {
    /// The integer type of `x::int2`, `x::int4` or `x::int8`, and their aliases.
    fn of(cast: &nodes::TypeCast) -> Option<Self> {
        let type_name = cast.type_name()?;
        if type_name.setof
            || type_name.pct_type
            || !type_name.typmods().is_empty()
            || !type_name.array_bounds().is_empty()
        {
            return None;
        }
        let names = type_name
            .names()
            .iter()
            .map(|name| name.sval())
            .collect::<Vec<_>>();
        let name = match names.as_slice() {
            [Some(name)] | [Some("pg_catalog"), Some(name)] => *name,
            _ => return None,
        };
        match name {
            "int2" => Some(Self::Int2),
            "int4" => Some(Self::Int4),
            "int8" => Some(Self::Int8),
            _ => None,
        }
    }

    fn fits(self, value: i64) -> bool {
        match self {
            Self::Int2 => i16::try_from(value).is_ok(),
            Self::Int4 => i32::try_from(value).is_ok(),
            Self::Int8 => true,
        }
    }
}

fn operands<'a>(expr: &'a nodes::A_Expr) -> impl Iterator<Item = Node<'a>> {
    [expr.lexpr(), expr.rexpr()]
        .into_iter()
        .filter(|node| !matches!(node, Node::None))
}

/// Whether Postgres types this value as an integer, whatever type the client
/// declares for its parameters.
fn is_integer_typed(node: Node<'_>) -> bool {
    match node {
        Node::A_Const(_) | Node::TypeCast(_) => true,
        Node::A_Expr(expr) => operands(expr).all(is_integer_typed),
        _ => false,
    }
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
            ("SELECT 1 LIMIT 10 / 3", Some(3)),
            ("SELECT 1 LIMIT 10 % 4", Some(2)),
            ("SELECT 1 LIMIT -(-2)", Some(2)),
            ("SELECT 1 LIMIT +3", Some(3)),
            ("SELECT 1 LIMIT (2 + 3) * 2", Some(10)),
            ("SELECT 1 LIMIT 10000000000", Some(10_000_000_000)),
            ("SELECT 1 LIMIT 1 + NULL", None),
            ("SELECT 1 LIMIT ALL", None),
        ] {
            assert_eq!(limit_count(sql), expected, "{sql}");
        }
    }

    #[test]
    fn evaluates_integer_casts() {
        for (sql, expected) in [
            ("SELECT 1 LIMIT 5::int", Some(5)),
            ("SELECT 1 LIMIT 5::smallint", Some(5)),
            ("SELECT 1 LIMIT CAST(5 AS bigint)", Some(5)),
            ("SELECT 1 LIMIT 5::int4 + 1::int8", Some(6)),
            ("SELECT 1 LIMIT 3000000000::bigint", Some(3_000_000_000)),
            // Postgres raises "out of range" on the shards.
            ("SELECT 1 LIMIT 40000::smallint", None),
            ("SELECT 1 LIMIT 3000000000::int", None),
        ] {
            assert_eq!(limit_count(sql), expected, "{sql}");
        }
    }

    #[test]
    fn errors_postgres_raises_are_left_to_the_shards() {
        for sql in [
            "SELECT 1 LIMIT 1 / 0",
            "SELECT 1 LIMIT 1 % 0",
            "SELECT 1 LIMIT 9223372036854775807 + 1",
            "SELECT 1 LIMIT -9223372036854775807 - 2",
            "SELECT 1 LIMIT -1",
        ] {
            assert_eq!(limit_count(sql), None, "{sql}");
        }
        assert_eq!(
            limit_count("SELECT 1 LIMIT -9223372036854775808 % -1"),
            Some(0)
        );
    }

    #[test]
    fn evaluates_cast_parameters() {
        let text = Bind::new_params("", &[Parameter::new(b"5"), Parameter::new(b"2")]);
        let text = limit("SELECT 1 LIMIT $1::int + 1 OFFSET $2::int", Some(&text)).unwrap();
        assert_eq!(text.limit, Some(6));
        assert_eq!(text.offset, Some(2));

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
    }

    #[test]
    fn rejects_values_pgdog_cannot_compute() {
        for sql in [
            "SELECT 1 LIMIT (SELECT 1)",
            "SELECT 1 LIMIT random()::int",
            "SELECT 1 LIMIT '1'",
            "SELECT 1 LIMIT 2.5",
            "SELECT 1 LIMIT 2.5::int",
            "SELECT 1 LIMIT 5::numeric",
            "SELECT 1 LIMIT 5::int[]",
            "SELECT 1 LIMIT 2 ^ 2",
            "SELECT 1 LIMIT 5 OPERATOR(pg_catalog.+) 1",
            // `$1` is numeric if the client declares it so, then 5 / 2 is 2.5.
            "SELECT 1 LIMIT $1 / 2",
            "SELECT 1 LIMIT ($1 + 1) / 2",
        ] {
            assert!(!computable(sql), "{sql}");
        }
        for sql in [
            "SELECT 1 LIMIT $1::int / 2",
            "SELECT 1 LIMIT $1 + 1",
            "SELECT 1 LIMIT $1 * 2",
        ] {
            assert!(computable(sql), "{sql}");
        }
    }
}
