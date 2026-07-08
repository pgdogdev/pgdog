//! Value extracted from a query.

use std::fmt::Display;

#[cfg(feature = "new_parser")]
use itertools::*;
use pg_query::{
    NodeEnum,
    protobuf::{Node as PgNode, a_const::Val, *},
};
#[cfg(feature = "new_parser")]
use pg_raw_parse::{Node, nodes};

use crate::net::{messages::Vector, vector::str_to_vector};

/// A value extracted from a query.
#[derive(Debug, Clone, PartialEq)]
pub enum Value<'a> {
    String(&'a str),
    Integer(i64),
    // FIXME(sage): This will lose precision on numeric
    Float(f64),
    Boolean(bool),
    Null,
    Placeholder(i32),
    Vector(Vector),
}

impl Display for Value<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::String(s) => write!(f, "'{}'", s.replace("'", "''")),
            Self::Integer(i) => write!(f, "{}", i),
            Self::Float(s) => write!(f, "{}", s),
            Self::Null => write!(f, "NULL"),
            Self::Boolean(b) => write!(f, "{}", if *b { "true" } else { "false" }),
            Self::Vector(v) => write!(
                f,
                "{}",
                v.iter()
                    .map(|v| v.to_string())
                    .collect::<Vec<_>>()
                    .join(",")
            ),
            Self::Placeholder(p) => write!(f, "${}", p),
        }
    }
}

impl Value<'_> {
    /// Get vector if it's a vector.
    #[cfg(test)]
    fn vector(self) -> Option<Vector> {
        match self {
            Self::Vector(vector) => Some(vector),
            _ => None,
        }
    }
}

#[cfg(feature = "new_parser")]
impl<'a> From<&'a nodes::A_Const> for Value<'a> {
    fn from(node: &'a nodes::A_Const) -> Self {
        use pg_raw_parse::const_val::ConstValue;
        match node.val() {
            Some(ConstValue::String(s)) if s.starts_with('[') && s.ends_with(']') => {
                str_to_vector(s)
                    .map(Value::Vector)
                    .unwrap_or(Value::String(s))
            }
            Some(ConstValue::String(s)) => {
                s.parse().map(Value::Integer).unwrap_or(Value::String(s))
            }

            Some(ConstValue::Boolean(b)) => Value::Boolean(b),
            Some(ConstValue::Integer(i)) => Value::Integer(i.into()),
            Some(ConstValue::Float(f)) if f.contains('.') => {
                f.parse().map(Value::Float).unwrap_or(Value::String(f))
            }
            Some(c @ ConstValue::Float(f)) => c
                .numeric_value()
                .map(Value::Integer)
                .unwrap_or(Value::String(f)),
            Some(ConstValue::BitString(bs)) => Value::String(bs),
            _ => Value::Null,
        }
    }
}

impl<'a> From<&'a AConst> for Value<'a> {
    fn from(value: &'a AConst) -> Self {
        if value.isnull {
            return Value::Null;
        }

        match value.val.as_ref() {
            Some(Val::Sval(s)) => {
                if s.sval.starts_with('[') && s.sval.ends_with(']') {
                    match str_to_vector(s.sval.as_str()) {
                        Ok(vector) => Value::Vector(vector),
                        _ => Value::String(s.sval.as_str()),
                    }
                } else {
                    match s.sval.parse::<i64>() {
                        Ok(i) => Value::Integer(i),
                        Err(_) => Value::String(s.sval.as_str()),
                    }
                }
            }
            Some(Val::Boolval(b)) => Value::Boolean(b.boolval),
            Some(Val::Ival(i)) => Value::Integer(i.ival as i64),
            Some(Val::Fval(Float { fval })) => {
                if fval.contains(".") {
                    if let Ok(float) = fval.parse() {
                        Value::Float(float)
                    } else {
                        Value::String(fval.as_str())
                    }
                } else {
                    match fval.parse::<i64>() {
                        Ok(i) => Value::Integer(i), // Integers over 2.2B and under -2.2B are sent as "floats"
                        Err(_) => Value::String(fval.as_str()),
                    }
                }
            }
            Some(Val::Bsval(bsval)) => Value::String(bsval.bsval.as_str()),
            None => Value::Null,
        }
    }
}

impl<'a> TryFrom<&'a PgNode> for Value<'a> {
    type Error = ();

    fn try_from(value: &'a PgNode) -> Result<Self, Self::Error> {
        Value::try_from(&value.node)
    }
}

#[cfg(feature = "new_parser")]
impl<'a> TryFrom<Node<'a>> for Value<'a> {
    type Error = ();

    fn try_from(value: Node<'a>) -> Result<Self, Self::Error> {
        use pg_raw_parse::nodes::A_Expr_Kind::AEXPR_OP;

        match value {
            Node::A_Const(c) => Ok(Self::from(c)),
            Node::ParamRef(pr) => Ok(Self::Placeholder(pr.number)),
            Node::TypeCast(c) => Self::try_from(c.arg()),
            Node::A_Expr(expr @ nodes::A_Expr { kind: AEXPR_OP, .. })
                if let Ok(n) = expr.name().into_iter().exactly_one()
                    && n.as_str() == Some("-")
                    && let Ok(Self::Float(float)) = expr.rexpr().try_into() =>
            {
                Ok(Self::Float(-float))
            }
            _ => Err(()),
        }
    }
}

impl<'a> TryFrom<&'a Option<NodeEnum>> for Value<'a> {
    type Error = ();

    fn try_from(value: &'a Option<NodeEnum>) -> Result<Self, Self::Error> {
        Ok(match value {
            Some(NodeEnum::AConst(a_const)) => a_const.into(),
            Some(NodeEnum::ParamRef(param_ref)) => Value::Placeholder(param_ref.number),
            Some(NodeEnum::TypeCast(cast)) => {
                if let Some(ref arg) = cast.arg {
                    Value::try_from(&arg.node)?
                } else {
                    Value::Null
                }
            }

            Some(NodeEnum::AExpr(expr)) => {
                if expr.kind() == AExprKind::AexprOp
                    && let Some(PgNode {
                        node: Some(NodeEnum::String(pg_query::protobuf::String { sval })),
                    }) = expr.name.first()
                    && sval == "-"
                    && let Some(ref node) = expr.rexpr
                {
                    let value = Value::try_from(&node.node)?;
                    if let Value::Float(float) = value {
                        return Ok(Value::Float(-float));
                    }
                }

                return Err(());
            }

            _ => return Err(()),
        })
    }
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    #[cfg(feature = "new_parser")]
    fn test_vector_value() {
        use pgdog_vector::Float;

        let result = pg_raw_parse::parse("SELECT '[1,2,3]'").unwrap();
        let node = selected_expr(&result);
        let vector = Value::try_from(node).unwrap();
        assert_eq!(
            vector.vector().unwrap(),
            Vector {
                values: vec![Float(1.0), Float(2.0), Float(3.0)]
            }
        );
    }

    #[test]
    #[cfg(not(feature = "new_parser"))]
    fn test_vector_value() {
        let a_cosnt = AConst {
            val: Some(Val::Sval(String {
                sval: "[1,2,3]".into(),
            })),
            isnull: false,
            location: 0,
        };
        let node = PgNode {
            node: Some(NodeEnum::AConst(a_cosnt)),
        };
        let vector = Value::try_from(&node).unwrap();
        assert_eq!(vector.vector().unwrap()[0], 1.0.into());
    }

    #[test]
    #[cfg(feature = "new_parser")]
    fn test_negative_numeric_with_cast() {
        // This will be parsed as a unary negation on a cast node, not a cast on
        // a negative numeric constant
        let result = pg_raw_parse::parse("SELECT -987654321.123456789::NUMERIC").unwrap();
        let neg_numeric_node = selected_expr(&result);

        let value = Value::try_from(neg_numeric_node).unwrap();
        assert_eq!(value, Value::Float(-987_654_321.123_456_8));
    }

    #[test]
    #[cfg(not(feature = "new_parser"))]
    fn test_negative_numeric_with_cast() {
        let stmt =
            pg_query::parse("INSERT INTO t (id, val) VALUES (2, -987654321.123456789::NUMERIC)")
                .unwrap();

        let insert = match stmt.protobuf.stmts[0].stmt.as_ref().unwrap().node.as_ref() {
            Some(NodeEnum::InsertStmt(insert)) => insert,
            _ => panic!("expected InsertStmt"),
        };

        let select = insert.select_stmt.as_ref().unwrap();
        let values = match select.node.as_ref() {
            Some(NodeEnum::SelectStmt(s)) => &s.values_lists,
            _ => panic!("expected SelectStmt"),
        };

        // values_lists[0] is a List node containing the tuple items
        let tuple = match values[0].node.as_ref() {
            Some(NodeEnum::List(list)) => &list.items,
            _ => panic!("expected List"),
        };

        // Second value in the VALUES tuple is our negative numeric
        let neg_numeric_node = &tuple[1];
        let value = Value::try_from(&neg_numeric_node.node).unwrap();

        assert_eq!(value, Value::Float(-987_654_321.123_456_8));
    }

    #[cfg(feature = "new_parser")]
    fn selected_expr(result: &pg_raw_parse::ParseResult) -> Node<'_> {
        let stmt = result.stmts().exactly_one().ok().unwrap();
        match stmt {
            Node::SelectStmt(s) => s.target_list().into_iter().exactly_one().unwrap().val(),
            _ => unreachable!(),
        }
    }
}
