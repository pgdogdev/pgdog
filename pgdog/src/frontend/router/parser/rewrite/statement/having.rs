//! Route-dependent rewrite for cross-shard `HAVING`.

use std::collections::HashSet;

use itertools::Itertools;
use pg_raw_parse::{ConstValue, Node, make, nodes};

use crate::frontend::router::parser::{
    AggregateFunction, CompareOperator, Function, HavingExpr, HavingRewritePlan, HavingValue,
};

use super::Error;

const ALIAS_PREFIX: &str = "__pgdog_having_";

struct ParsedHaving<'a> {
    expression: HavingTemplate,
    values: Vec<(String, Node<'a>)>,
}

enum HavingTemplate {
    Compare {
        left: ValueTemplate,
        operator: CompareOperator,
        right: ValueTemplate,
    },
}

struct ValueTemplate {
    alias: String,
}

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
        .filter_map(|target| {
            target
                .name()
                .or_else(|| target_output_name(target.val()))
                .map(str::to_owned)
        })
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
                parser.values.push((alias, group));
            }
            Node::A_Const(_) => {}
            _ => {
                return Err(Error::UnsupportedHaving(
                    "GROUP BY expressions other than columns and ordinals are unsupported",
                ));
            }
        }
    }
    let parsed = ParsedHaving {
        expression,
        values: parser.values,
    };

    let hidden_aliases = parsed
        .values
        .iter()
        .map(|(alias, _)| alias.clone())
        .collect::<Vec<_>>();
    let targets = parsed
        .values
        .iter()
        .map(|(alias, node)| mem.make_res_target(Some(alias), mem.empty(), mem.make_unique(*node)))
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

    Ok(HavingRewritePlan::new(
        parsed.expression.materialize(),
        hidden_aliases,
    ))
}

fn target_output_name(node: Node<'_>) -> Option<&str> {
    match node {
        Node::ColumnRef(column) => column.fields().iter().next_back()?.as_str(),
        Node::FuncCall(function) => function.funcname().iter().next_back()?.as_str(),
        Node::TypeCast(cast) => target_output_name(cast.arg()),
        _ => None,
    }
}

impl HavingTemplate {
    fn materialize(self) -> HavingExpr {
        match self {
            Self::Compare {
                left,
                operator,
                right,
            } => HavingExpr::Compare {
                left: left.materialize(),
                operator,
                right: right.materialize(),
            },
        }
    }
}

impl ValueTemplate {
    fn materialize(self) -> HavingValue {
        HavingValue::new(self.alias)
    }
}

struct Parser<'a> {
    aliases: HashSet<String>,
    values: Vec<(String, Node<'a>)>,
    next_alias: usize,
}

impl<'a> Parser<'a> {
    fn parse_expression(&mut self, node: Node<'a>) -> Result<HavingTemplate, Error> {
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
                Ok(HavingTemplate::Compare {
                    left: self.parse_value(expression.lexpr())?,
                    operator,
                    right: self.parse_value(expression.rexpr())?,
                })
            }
            _ => Err(Error::UnsupportedHaving(
                "only a single comparison is supported",
            )),
        }
    }

    fn parse_value(&mut self, node: Node<'a>) -> Result<ValueTemplate, Error> {
        if !Self::supported_value(node) {
            return Err(Error::UnsupportedHaving(
                "operands must be aggregate calls, grouped columns, constants or parameters",
            ));
        }

        let alias = self.next_alias();
        self.values.push((alias.clone(), node));
        Ok(ValueTemplate { alias })
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
             $1 AS __pgdog_having_1, customer_id AS __pgdog_having_2 \
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
