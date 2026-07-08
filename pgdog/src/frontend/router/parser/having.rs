//! HAVING clause parser for cross-shard post-processing.

use pg_query::{
    NodeEnum,
    protobuf::{AExprKind, BoolExprType, NullTestType, SelectStmt},
};

use super::{Aggregate, AggregateFunction, Function, Value};

#[derive(Debug, Clone, PartialEq)]
pub(crate) enum CompareOp {
    Eq,
    NotEq,
    Lt,
    Lte,
    Gt,
    Gte,
}

#[derive(Debug, Clone, PartialEq)]
pub(crate) enum HavingLiteral {
    Integer(i64),
    Float(f64),
    Boolean(bool),
    String(String),
    Null,
}

impl From<Value<'_>> for HavingLiteral {
    fn from(value: Value<'_>) -> Self {
        match value {
            Value::String(s) => Self::String(s.to_owned()),
            Value::Integer(i) => Self::Integer(i),
            Value::Float(f) => Self::Float(f),
            Value::Boolean(b) => Self::Boolean(b),
            Value::Null => Self::Null,
            Value::Placeholder(_) => Self::Null,
            Value::Vector(_) => Self::Null,
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub(crate) enum HavingValueTemplate {
    ColumnIndex(usize),
    ColumnName(String),
    Param(usize),
    Literal(HavingLiteral),
    Expression(Box<pg_query::protobuf::Node>),
}

#[derive(Debug, Clone, PartialEq)]
pub(crate) enum HavingValue {
    ColumnIndex(usize),
    ColumnName(String),
    Literal(HavingLiteral),
}

#[derive(Debug, Clone, PartialEq)]
pub(crate) enum HavingExprTemplate {
    Compare {
        left: Box<HavingValueTemplate>,
        op: CompareOp,
        right: Box<HavingValueTemplate>,
    },
    And(Vec<HavingExprTemplate>),
    Or(Vec<HavingExprTemplate>),
    IsNull {
        value: Box<HavingValueTemplate>,
        not: bool,
    },
}

#[derive(Debug, Clone, PartialEq)]
pub(crate) enum HavingExpr {
    Compare {
        left: HavingValue,
        op: CompareOp,
        right: HavingValue,
    },
    And(Vec<HavingExpr>),
    Or(Vec<HavingExpr>),
    IsNull {
        value: HavingValue,
        not: bool,
    },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum HavingParseError {
    Unsupported,
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub(crate) struct HavingRewritePlan {
    hidden_aliases: Vec<String>,
}

impl HavingRewritePlan {
    pub(crate) fn is_noop(&self) -> bool {
        self.hidden_aliases.is_empty()
    }

    pub(crate) fn hidden_aliases(&self) -> &[String] {
        &self.hidden_aliases
    }

    pub(crate) fn add_hidden_alias(&mut self, alias: String) {
        self.hidden_aliases.push(alias);
    }
}

impl HavingExprTemplate {
    pub(crate) fn parse(
        stmt: &SelectStmt,
        aggregate: &Aggregate,
    ) -> Result<Option<Self>, HavingParseError> {
        let Some(having) = &stmt.having_clause else {
            return Ok(None);
        };
        Self::parse_node(stmt, aggregate, having).map(Some)
    }

    pub(crate) fn param_indices(&self) -> Vec<usize> {
        let mut params = vec![];
        self.collect_params(&mut params);
        params.sort_unstable();
        params.dedup();
        params
    }

    fn collect_params(&self, params: &mut Vec<usize>) {
        match self {
            Self::Compare { left, right, .. } => {
                Self::collect_value_params(left.as_ref(), params);
                Self::collect_value_params(right.as_ref(), params);
            }
            Self::And(children) | Self::Or(children) => {
                for child in children {
                    child.collect_params(params);
                }
            }
            Self::IsNull { value, .. } => Self::collect_value_params(value.as_ref(), params),
        }
    }

    fn collect_value_params(value: &HavingValueTemplate, params: &mut Vec<usize>) {
        if let HavingValueTemplate::Param(index) = value {
            params.push(*index);
        }
    }

    pub(crate) fn resolve_with(
        &self,
        lookup: &impl Fn(usize) -> Option<HavingLiteral>,
    ) -> Option<HavingExpr> {
        match self {
            HavingExprTemplate::Compare { left, op, right } => Some(HavingExpr::Compare {
                left: Self::resolve_value(left.as_ref(), lookup)?,
                op: op.clone(),
                right: Self::resolve_value(right.as_ref(), lookup)?,
            }),
            HavingExprTemplate::And(children) => {
                let children = children
                    .iter()
                    .map(|child| child.resolve_with(lookup))
                    .collect::<Option<Vec<_>>>()?;
                Some(HavingExpr::And(children))
            }
            HavingExprTemplate::Or(children) => {
                let children = children
                    .iter()
                    .map(|child| child.resolve_with(lookup))
                    .collect::<Option<Vec<_>>>()?;
                Some(HavingExpr::Or(children))
            }
            HavingExprTemplate::IsNull { value, not } => Some(HavingExpr::IsNull {
                value: Self::resolve_value(value.as_ref(), lookup)?,
                not: *not,
            }),
        }
    }

    fn resolve_value(
        value: &HavingValueTemplate,
        lookup: &impl Fn(usize) -> Option<HavingLiteral>,
    ) -> Option<HavingValue> {
        match value {
            HavingValueTemplate::ColumnIndex(index) => Some(HavingValue::ColumnIndex(*index)),
            HavingValueTemplate::ColumnName(name) => Some(HavingValue::ColumnName(name.clone())),
            HavingValueTemplate::Literal(value) => Some(HavingValue::Literal(value.clone())),
            HavingValueTemplate::Param(index) => lookup(*index).map(HavingValue::Literal),
            HavingValueTemplate::Expression(_) => None,
        }
    }

    fn parse_node(
        stmt: &SelectStmt,
        aggregate: &Aggregate,
        node: &pg_query::protobuf::Node,
    ) -> Result<Self, HavingParseError> {
        match node.node.as_ref() {
            Some(NodeEnum::BoolExpr(bool_expr)) => {
                let parsed = bool_expr
                    .args
                    .iter()
                    .map(|arg| Self::parse_node(stmt, aggregate, arg))
                    .collect::<Result<Vec<_>, _>>()?;
                match bool_expr.boolop() {
                    BoolExprType::AndExpr => Ok(Self::And(parsed)),
                    BoolExprType::OrExpr => Ok(Self::Or(parsed)),
                    _ => Err(HavingParseError::Unsupported),
                }
            }
            Some(NodeEnum::AExpr(expr)) => {
                if expr.kind() != AExprKind::AexprOp {
                    return Err(HavingParseError::Unsupported);
                }
                let op = Self::parse_compare_op(expr.name.first())?;
                let left = expr
                    .lexpr
                    .as_ref()
                    .ok_or(HavingParseError::Unsupported)
                    .and_then(|node| Self::parse_value(stmt, aggregate, node))?;
                let right = expr
                    .rexpr
                    .as_ref()
                    .ok_or(HavingParseError::Unsupported)
                    .and_then(|node| Self::parse_value(stmt, aggregate, node))?;
                Ok(Self::Compare {
                    left: Box::new(left),
                    op,
                    right: Box::new(right),
                })
            }
            Some(NodeEnum::NullTest(test)) => {
                let value = test
                    .arg
                    .as_ref()
                    .ok_or(HavingParseError::Unsupported)
                    .and_then(|node| Self::parse_value(stmt, aggregate, node))?;
                let not = matches!(test.nulltesttype(), NullTestType::IsNotNull);
                if matches!(
                    test.nulltesttype(),
                    NullTestType::IsNull | NullTestType::IsNotNull
                ) {
                    Ok(Self::IsNull {
                        value: Box::new(value),
                        not,
                    })
                } else {
                    Err(HavingParseError::Unsupported)
                }
            }
            _ => Err(HavingParseError::Unsupported),
        }
    }

    fn parse_value(
        stmt: &SelectStmt,
        aggregate: &Aggregate,
        node: &pg_query::protobuf::Node,
    ) -> Result<HavingValueTemplate, HavingParseError> {
        match node.node.as_ref() {
            Some(NodeEnum::TypeCast(cast)) => cast
                .arg
                .as_ref()
                .ok_or(HavingParseError::Unsupported)
                .and_then(|arg| Self::parse_value(stmt, aggregate, arg)),
            Some(NodeEnum::AConst(_)) => {
                let value = Value::try_from(node).map_err(|_| HavingParseError::Unsupported)?;
                Ok(HavingValueTemplate::Literal(value.into()))
            }
            Some(NodeEnum::ParamRef(param)) => {
                if param.number <= 0 {
                    return Err(HavingParseError::Unsupported);
                }
                Ok(HavingValueTemplate::Param((param.number - 1) as usize))
            }
            Some(NodeEnum::ColumnRef(column_ref)) => {
                let fields = column_ref
                    .fields
                    .iter()
                    .filter_map(|field| match field.node.as_ref() {
                        Some(NodeEnum::String(string)) => Some(string.sval.as_str()),
                        _ => None,
                    })
                    .collect::<Vec<_>>();

                if let Some(name) = fields.last() {
                    if let Some(index) = Self::target_index_by_alias(stmt, name) {
                        return Ok(HavingValueTemplate::ColumnIndex(index));
                    }
                    return Ok(HavingValueTemplate::Expression(Box::new(node.clone())));
                }
                Err(HavingParseError::Unsupported)
            }
            Some(NodeEnum::FuncCall(_)) => {
                let func = Function::try_from(node).map_err(|_| HavingParseError::Unsupported)?;
                let agg_fn = Self::aggregate_function_from_name(func.name)
                    .ok_or(HavingParseError::Unsupported)?;
                let candidates = aggregate
                    .targets()
                    .iter()
                    .filter(|target| target.function() == &agg_fn)
                    .map(|target| target.column())
                    .collect::<Vec<_>>();

                if candidates.len() == 1 {
                    Ok(HavingValueTemplate::ColumnIndex(candidates[0]))
                } else {
                    Ok(HavingValueTemplate::Expression(Box::new(node.clone())))
                }
            }
            Some(NodeEnum::ResTarget(res_target)) => res_target
                .val
                .as_ref()
                .ok_or(HavingParseError::Unsupported)
                .and_then(|value| Self::parse_value(stmt, aggregate, value)),
            _ => Err(HavingParseError::Unsupported),
        }
    }

    fn parse_compare_op(
        node: Option<&pg_query::protobuf::Node>,
    ) -> Result<CompareOp, HavingParseError> {
        let Some(NodeEnum::String(string)) = node.and_then(|node| node.node.as_ref()) else {
            return Err(HavingParseError::Unsupported);
        };
        match string.sval.as_str() {
            "=" => Ok(CompareOp::Eq),
            "<>" | "!=" => Ok(CompareOp::NotEq),
            "<" => Ok(CompareOp::Lt),
            "<=" => Ok(CompareOp::Lte),
            ">" => Ok(CompareOp::Gt),
            ">=" => Ok(CompareOp::Gte),
            _ => Err(HavingParseError::Unsupported),
        }
    }

    fn target_index_by_alias(stmt: &SelectStmt, alias: &str) -> Option<usize> {
        stmt.target_list
            .iter()
            .position(|target| match target.node.as_ref() {
                Some(NodeEnum::ResTarget(res)) => res.name == alias,
                _ => false,
            })
    }

    fn aggregate_function_from_name(name: &str) -> Option<AggregateFunction> {
        match name {
            "count" => Some(AggregateFunction::Count),
            "max" => Some(AggregateFunction::Max),
            "min" => Some(AggregateFunction::Min),
            "sum" => Some(AggregateFunction::Sum),
            "avg" => Some(AggregateFunction::Avg),
            "stddev" | "stddev_samp" => Some(AggregateFunction::StddevSamp),
            "stddev_pop" => Some(AggregateFunction::StddevPop),
            "variance" | "var_samp" => Some(AggregateFunction::VarSamp),
            "var_pop" => Some(AggregateFunction::VarPop),
            _ => None,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::backend::schema::Schema;
    use pg_query::{NodeEnum, parse};

    fn parse_select(sql: &str) -> SelectStmt {
        let stmt = parse(sql).unwrap().protobuf.stmts.remove(0).stmt.unwrap();
        match stmt.node.unwrap() {
            NodeEnum::SelectStmt(stmt) => *stmt,
            _ => panic!("not a select"),
        }
    }

    #[test]
    fn parses_simple_count_having() {
        let stmt = parse_select(
            "SELECT user_id, COUNT(*) AS c FROM t GROUP BY user_id HAVING COUNT(*) > 2",
        );
        let aggregate = Aggregate::parse(&stmt, &Schema::default());
        let having = HavingExprTemplate::parse(&stmt, &aggregate)
            .unwrap()
            .expect("having");
        match having {
            HavingExprTemplate::Compare { .. } => {}
            _ => panic!("expected compare expression"),
        }
    }

    #[test]
    fn resolves_params() {
        let stmt = parse_select(
            "SELECT user_id, COUNT(*) AS c FROM t GROUP BY user_id HAVING COUNT(*) > $1",
        );
        let aggregate = Aggregate::parse(&stmt, &Schema::default());
        let having = HavingExprTemplate::parse(&stmt, &aggregate)
            .unwrap()
            .expect("having");
        let resolved = having.resolve_with(&|idx| {
            if idx == 0 {
                Some(HavingLiteral::Integer(2))
            } else {
                None
            }
        });
        assert!(resolved.is_some());
    }
}
