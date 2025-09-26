use std::collections::HashMap;

use pg_query::{protobuf::AExprKind, protobuf::TypeName, Node, NodeEnum};

#[derive(Debug, Default)]
pub struct ExpressionRegistry {
    entries: HashMap<CanonicalExpr, usize>,
    next_id: usize,
}

impl ExpressionRegistry {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn intern(&mut self, node: &Node) -> usize {
        let canonical = CanonicalExpr::from_node(node);
        *self.entries.entry(canonical).or_insert_with(|| {
            let id = self.next_id;
            self.next_id += 1;
            id
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord)]
enum CanonicalExpr {
    Column(Vec<String>),
    Function {
        name: Vec<String>,
        args: Vec<CanonicalExpr>,
        distinct: bool,
        star: bool,
    },
    TypeCast {
        expr: Box<CanonicalExpr>,
        target: Vec<String>,
    },
    Constant(String),
    Parameter(i32),
    Operation {
        operator: String,
        operands: Vec<CanonicalExpr>,
    },
    Other(String),
}

impl CanonicalExpr {
    fn from_node(node: &Node) -> Self {
        let Some(inner) = node.node.as_ref() else {
            return CanonicalExpr::Other(String::new());
        };

        match inner {
            NodeEnum::ColumnRef(column) => CanonicalExpr::Column(
                column
                    .fields
                    .iter()
                    .map(|field| match field.node.as_ref() {
                        Some(NodeEnum::String(string)) => string.sval.to_lowercase(),
                        Some(NodeEnum::AStar(_)) => "*".to_string(),
                        Some(other) => format!("{:?}", other),
                        None => String::new(),
                    })
                    .collect(),
            ),
            NodeEnum::ParamRef(param) => CanonicalExpr::Parameter(param.number),
            NodeEnum::AConst(constant) => CanonicalExpr::Constant(format!("{:?}", constant.val)),
            NodeEnum::FuncCall(func) => {
                let mut name = Vec::new();
                for ident in &func.funcname {
                    match ident.node.as_ref() {
                        Some(NodeEnum::String(string)) => name.push(string.sval.to_lowercase()),
                        Some(other) => name.push(format!("{:?}", other)),
                        None => name.push(String::new()),
                    }
                }

                let mut args = func
                    .args
                    .iter()
                    .map(CanonicalExpr::from_node)
                    .collect::<Vec<_>>();

                if func.agg_distinct {
                    // DISTINCT aggregate arguments form a set, so sort them to
                    // ensure canonical equality across permutations. Non-DISTINCT
                    // functions must retain argument order because most built-ins
                    // are not commutative.
                    args.sort();
                }

                CanonicalExpr::Function {
                    name,
                    args,
                    distinct: func.agg_distinct,
                    star: func.agg_star,
                }
            }
            NodeEnum::TypeCast(cast) => {
                let expr = cast
                    .arg
                    .as_ref()
                    .map(|node| CanonicalExpr::from_node(node.as_ref()))
                    .unwrap_or_else(|| CanonicalExpr::Other(String::new()));
                let target = format_type_name(cast.type_name.as_ref());
                CanonicalExpr::TypeCast {
                    expr: Box::new(expr),
                    target,
                }
            }
            NodeEnum::AExpr(expr) => {
                let operator = expr
                    .name
                    .iter()
                    .filter_map(|node| match node.node.as_ref() {
                        Some(NodeEnum::String(string)) => Some(string.sval.to_lowercase()),
                        _ => None,
                    })
                    .collect::<Vec<_>>()
                    .join(" ");

                let mut operands = Vec::new();
                if let Some(lexpr) = expr.lexpr.as_ref() {
                    operands.push(CanonicalExpr::from_node(lexpr.as_ref()));
                }
                if let Some(rexpr) = expr.rexpr.as_ref() {
                    operands.push(CanonicalExpr::from_node(rexpr.as_ref()));
                }

                if is_commutative(&expr.kind(), operator.as_str()) {
                    operands.sort();
                }

                CanonicalExpr::Operation { operator, operands }
            }
            NodeEnum::ResTarget(res) => res
                .val
                .as_ref()
                .map(|node| CanonicalExpr::from_node(node.as_ref()))
                .unwrap_or_else(|| CanonicalExpr::Other(String::from("restarget"))),
            other => CanonicalExpr::Other(format!("{:?}", other)),
        }
    }
}

fn format_type_name(type_name: Option<&TypeName>) -> Vec<String> {
    let Some(type_name) = type_name else {
        return vec![];
    };

    type_name
        .names
        .iter()
        .map(|node| match node.node.as_ref() {
            Some(NodeEnum::String(string)) => string.sval.to_lowercase(),
            Some(other) => format!("{:?}", other),
            None => String::new(),
        })
        .collect()
}

fn is_commutative(kind: &AExprKind, operator: &str) -> bool {
    let op = operator.trim().to_lowercase();
    matches!(
        (kind, op.as_str()),
        (AExprKind::AexprOp, "+")
            | (AExprKind::AexprOp, "*")
            | (AExprKind::AexprOp, "and")
            | (AExprKind::AexprOp, "or")
            | (AExprKind::AexprOp, "=")
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    fn extract_targets(sql: &str) -> Vec<Node> {
        let ast = pg_query::parse(sql).unwrap();
        let stmt = ast
            .protobuf
            .stmts
            .first()
            .and_then(|s| s.stmt.as_ref())
            .and_then(|s| s.node.as_ref())
            .unwrap()
            .clone();

        match stmt {
            NodeEnum::SelectStmt(select) => select
                .target_list
                .into_iter()
                .filter_map(|target| match target.node {
                    Some(NodeEnum::ResTarget(res)) => res.val.map(|boxed| *boxed),
                    _ => None,
                })
                .collect(),
            _ => panic!("expected select statement"),
        }
    }

    #[test]
    fn registry_matches_identical_expressions() {
        let mut registry = ExpressionRegistry::new();
        let targets = extract_targets("SELECT price, price FROM menu");
        assert_eq!(targets.len(), 2);
        let a = registry.intern(&targets[0]);
        let b = registry.intern(&targets[1]);
        assert_eq!(a, b);
    }

    #[test]
    fn registry_matches_casted_equivalents() {
        let mut registry = ExpressionRegistry::new();
        let targets = extract_targets("SELECT price::numeric, price::numeric FROM menu");
        assert_eq!(targets.len(), 2);
        let a = registry.intern(&targets[0]);
        let b = registry.intern(&targets[1]);
        assert_eq!(a, b);
    }

    #[test]
    fn registry_matches_commutative_additions() {
        let mut registry = ExpressionRegistry::new();
        let targets = extract_targets("SELECT price + tax, tax + price FROM menu");
        assert_eq!(targets.len(), 2);
        let a = registry.intern(&targets[0]);
        let b = registry.intern(&targets[1]);
        assert_eq!(a, b);
    }

    #[test]
    fn registry_distinguishes_different_expressions() {
        let mut registry = ExpressionRegistry::new();
        let targets = extract_targets("SELECT price, cost FROM menu");
        assert_eq!(targets.len(), 2);
        let a = registry.intern(&targets[0]);
        let b = registry.intern(&targets[1]);
        assert_ne!(a, b);
    }

    #[test]
    fn registry_distinguishes_distinct() {
        let mut registry = ExpressionRegistry::new();
        let targets = extract_targets("SELECT COUNT(DISTINCT price), COUNT(price) FROM menu");
        assert_eq!(targets.len(), 2);
        let distinct = registry.intern(&targets[0]);
        let regular = registry.intern(&targets[1]);
        assert_ne!(distinct, regular);
    }

    #[test]
    fn registry_preserves_argument_order_for_functions() {
        let mut registry = ExpressionRegistry::new();
        let targets = extract_targets("SELECT substr(name, 1, 2), substr(name, 2, 1) FROM menu");
        assert_eq!(targets.len(), 2);
        let first = registry.intern(&targets[0]);
        let second = registry.intern(&targets[1]);
        assert_ne!(first, second);
    }
}
