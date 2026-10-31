//! Route-dependent rewrite for cross-shard `HAVING`.

use std::collections::HashSet;

use itertools::Itertools;
use pg_raw_parse::{ConstValue, Node, make, nodes};

use crate::frontend::router::parser::{
    AggregateFunction, CompareOperator, Function, HavingExpr, HavingRewritePlan, HavingValue,
};

use super::{Error, projection::target_output_name};

const ALIAS_PREFIX: &str = "__pgdog_having_";

/// Project every supported predicate operand and disable shard-local filtering.
///
/// The original expression remains under `OR TRUE` so PostgreSQL still performs
/// parse analysis and parameter type inference.
pub(super) fn rewrite_select<'a>(
    select: &mut nodes::SelectStmtMut<'a, '_>,
    mem: make::MemoryToken<'a>,
) -> Result<HavingRewritePlan, Error> {
    let having = select.having_clause();
    debug_assert!(
        !matches!(having, Node::None),
        "HAVING rewrite requires a HAVING clause"
    );
    let having_source = mem.make_unique(having);
    let having = having_source.as_ref();

    let aliases = select
        .target_list()
        .iter()
        .filter_map(|target| target_output_name(target).map(str::to_owned))
        .collect();
    let mut parser = Parser {
        aliases,
        values: Vec::new(),
        next_alias: 0,
    };
    let expression = parser.parse_expression(having)?;
    let group_sources = select
        .group_clause()
        .iter()
        .map(|group| mem.make_unique(group))
        .collect::<Vec<_>>();
    for group_source in &group_sources {
        let group = group_source.as_ref();
        match group {
            Node::ColumnRef(_) => {
                let alias = parser.next_alias();
                parser.values.push(Projected {
                    alias,
                    node: group,
                    typed_like: None,
                });
            }
            Node::A_Const(_) => {}
            _ => {
                return Err(Error::UnsupportedHaving(
                    "GROUP BY expressions other than columns and ordinals are unsupported",
                ));
            }
        }
    }

    let hidden_aliases = parser
        .values
        .iter()
        .map(|value| value.alias.clone())
        .collect::<Vec<_>>();
    let targets = parser
        .values
        .iter()
        .map(|value| {
            let projected = match value.typed_like {
                Some(like) => typed_like(mem, value.node, like),
                None => mem.make_unique(value.node),
            };
            mem.make_res_target(Some(&value.alias), mem.empty(), projected)
        })
        .collect::<Vec<_>>();
    select
        .target_list_mut()
        .extend(mem, mem.make_list(&targets));

    let original = mem.make_unique(having);
    let true_value = mem.make_a_const(ConstValue::Boolean(true)).uncast();
    let mut neutralized = mem.make_node::<nodes::BoolExpr>();
    neutralized
        .as_mut()
        .set_boolop(nodes::BoolExprType::OR_EXPR);
    neutralized
        .as_mut()
        .set_args(mem.make_list(&[original, true_value]));
    select.set_having_clause(neutralized.uncast());

    Ok(HavingRewritePlan::new(expression, hidden_aliases))
}

/// `CASE WHEN false THEN <like> ELSE <value> END` evaluates to `value` with the
/// type PostgreSQL infers for it in a comparison with `like`. A bare untyped
/// literal or parameter in the target list would be resolved as `text` instead.
fn typed_like<'a>(
    mem: make::MemoryToken<'a>,
    value: Node<'_>,
    like: Node<'_>,
) -> make::Unique<'a, Node<'a>> {
    let mut when = mem.make_node::<nodes::CaseWhen>();
    when.as_mut()
        .set_expr(mem.make_a_const(ConstValue::Boolean(false)).uncast());
    when.as_mut().set_result(mem.make_unique(like));
    let mut case = mem.make_node::<nodes::CaseExpr>();
    case.as_mut().set_args(mem.make_list(&[when.uncast()]));
    case.as_mut().set_defresult(mem.make_unique(value));
    case.uncast()
}

struct Projected<'a> {
    alias: String,
    node: Node<'a>,
    /// Operand whose type an untyped literal or parameter should take.
    typed_like: Option<Node<'a>>,
}

struct Parser<'a> {
    aliases: HashSet<String>,
    values: Vec<Projected<'a>>,
    next_alias: usize,
}

impl<'a> Parser<'a> {
    fn parse_expression(&mut self, node: Node<'a>) -> Result<HavingExpr, Error> {
        match node {
            Node::A_Expr(expression) if expression.kind == nodes::A_Expr_Kind::AEXPR_OP => {
                let operator = expression
                    .name()
                    .iter()
                    .exactly_one()
                    .ok()
                    .and_then(Node::as_str)
                    .and_then(Self::comparison_operator)
                    .ok_or_else(|| {
                        Error::UnsupportedHaving(
                            "only =, <>, !=, <, <=, > and >= comparisons are supported",
                        )
                    })?;
                let (left, right) = (expression.lexpr(), expression.rexpr());
                Ok(HavingExpr::Compare {
                    left: self.parse_value(left, right)?,
                    operator,
                    right: self.parse_value(right, left)?,
                })
            }
            _ => Err(Error::UnsupportedHaving(
                "only a single comparison is supported",
            )),
        }
    }

    fn parse_value(&mut self, node: Node<'a>, other: Node<'a>) -> Result<HavingValue, Error> {
        if !Self::supported_value(node) {
            return Err(Error::UnsupportedHaving(
                "operands must be aggregate calls, grouped columns, constants or parameters",
            ));
        }

        let untyped = |node| matches!(node, Node::A_Const(_) | Node::ParamRef(_));
        let alias = self.next_alias();
        self.values.push(Projected {
            alias: alias.clone(),
            node,
            typed_like: (untyped(node) && !untyped(other)).then_some(other),
        });
        Ok(HavingValue::new(alias))
    }

    fn supported_value(node: Node<'_>) -> bool {
        match node {
            Node::A_Const(_) | Node::ParamRef(_) | Node::ColumnRef(_) => true,
            Node::FuncCall(_) => Self::supported_aggregate(node),
            _ => false,
        }
    }

    fn supported_aggregate(node: Node<'_>) -> bool {
        Function::try_from(node)
            .is_ok_and(|function| AggregateFunction::from_builtin_name(function.name).is_some())
    }

    fn comparison_operator(operator: &str) -> Option<CompareOperator> {
        Some(match operator {
            "=" => CompareOperator::Equal,
            "<>" | "!=" => CompareOperator::NotEqual,
            "<" => CompareOperator::Less,
            "<=" => CompareOperator::LessOrEqual,
            ">" => CompareOperator::Greater,
            ">=" => CompareOperator::GreaterOrEqual,
            _ => return None,
        })
    }

    fn next_alias(&mut self) -> String {
        loop {
            let alias = format!("{ALIAS_PREFIX}{}", self.next_alias);
            self.next_alias += 1;
            if self.aliases.insert(alias.clone()) {
                return alias;
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn rewrite(sql: &str) -> (String, HavingRewritePlan) {
        let ast = pg_raw_parse::parse(sql).expect("parse HAVING query");
        let Node::SelectStmt(select) = ast.stmts().next().expect("one statement") else {
            panic!("expected SELECT")
        };
        let mut plan = None;
        let rewritten = make::owned(|mem| {
            let mut select = mem.make_unique(select);
            plan =
                Some(rewrite_select(&mut select.as_mut(), mem).expect("rewrite supported HAVING"));
            select
        });
        (
            pg_raw_parse::deparse(&*rewritten)
                .expect("deparse rewritten query")
                .as_str()
                .to_owned(),
            plan.expect("rewrite plan"),
        )
    }

    #[test]
    fn rewrites_single_comparison_with_parameter() {
        let (sql, plan) = rewrite(
            "SELECT customer_id, COUNT(*) FROM purchases \
             GROUP BY customer_id HAVING COUNT(*) > $1",
        );

        assert!(matches!(
            plan.expression(),
            HavingExpr::Compare {
                operator: CompareOperator::Greater,
                ..
            }
        ));
        assert_eq!(
            sql,
            "SELECT customer_id, count(*), count(*) AS __pgdog_having_0, \
             CASE WHEN false THEN count(*) ELSE $1 END AS __pgdog_having_1, \
             customer_id AS __pgdog_having_2 \
             FROM purchases GROUP BY customer_id HAVING count(*) > $1 OR true"
        );
        assert_eq!(plan.hidden_aliases().len(), 3);
    }

    #[test]
    fn rejects_boolean_null_test_and_cast_predicates() {
        for sql in [
            "SELECT customer_id FROM purchases GROUP BY customer_id \
             HAVING COUNT(*) > 1 AND COUNT(*) < 5",
            "SELECT customer_id FROM purchases GROUP BY customer_id \
             HAVING MAX(price) IS NULL",
            "SELECT customer_id FROM purchases GROUP BY customer_id \
             HAVING COUNT(*)::numeric > 3",
        ] {
            let ast = pg_raw_parse::parse(sql).unwrap();
            let Node::SelectStmt(select) = ast.stmts().next().unwrap() else {
                panic!("expected SELECT")
            };
            let result = make::try_owned(|mem| {
                let mut select = mem.make_unique(select);
                rewrite_select(&mut select.as_mut(), mem).map(|_| select)
            });
            assert!(matches!(result, Err(Error::UnsupportedHaving(_))), "{sql}");
        }
    }

    #[test]
    fn parses_every_comparison_operator() {
        for (sql, expected) in [
            ("=", CompareOperator::Equal),
            ("<>", CompareOperator::NotEqual),
            ("!=", CompareOperator::NotEqual),
            ("<", CompareOperator::Less),
            ("<=", CompareOperator::LessOrEqual),
            (">", CompareOperator::Greater),
            (">=", CompareOperator::GreaterOrEqual),
        ] {
            assert_eq!(Parser::comparison_operator(sql), Some(expected));
        }
        assert_eq!(Parser::comparison_operator("~"), None);
    }
}
