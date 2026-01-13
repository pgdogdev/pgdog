//! Value extracted from a query.

use std::fmt::Display;

use pg_query::{
    protobuf::{a_const::Val, *},
    NodeEnum,
};

use crate::net::{messages::Vector, vector::str_to_vector};

/// A value extracted from a query.
#[derive(Debug, Clone, PartialEq)]
pub enum Value<'a> {
    String(&'a str),
    Integer(i64),
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
    pub(crate) fn vector(self) -> Option<Vector> {
        match self {
            Self::Vector(vector) => Some(vector),
            _ => None,
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
                    if let Ok(vector) = str_to_vector(s.sval.as_str()) {
                        Value::Vector(vector)
                    } else {
                        Value::String(s.sval.as_str())
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

impl<'a> TryFrom<&'a Node> for Value<'a> {
    type Error = ();

    fn try_from(value: &'a Node) -> Result<Self, Self::Error> {
        Value::try_from(&value.node)
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
                if expr.kind() == AExprKind::AexprOp {
                    if let Some(Node {
                        node: Some(NodeEnum::String(pg_query::protobuf::String { sval })),
                    }) = expr.name.first()
                    {
                        if sval == "-" {
                            if let Some(ref node) = expr.rexpr {
                                let value = Value::try_from(&node.node)?;
                                if let Value::Float(float) = value {
                                    return Ok(Value::Float(-float));
                                }
                            }
                        }
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
    fn test_vector_value() {
        let a_cosnt = AConst {
            val: Some(Val::Sval(String {
                sval: "[1,2,3]".into(),
            })),
            isnull: false,
            location: 0,
        };
        let node = Node {
            node: Some(NodeEnum::AConst(a_cosnt)),
        };
        let vector = Value::try_from(&node).unwrap();
        assert_eq!(vector.vector().unwrap()[0], 1.0.into());
    }

    #[test]
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

        assert_eq!(value, Value::Float(-987654321.123456789));
    }
}
