use super::{Aggregate, AggregateFunction, HelperMapping, RewriteOutput, RewritePlan};
use pg_query::protobuf::{FuncCall, Node, ResTarget, String as PgString};
use pg_query::{NodeEnum, ParseResult};

/// Query rewrite engine. Currently supports injecting helper aggregates for AVG.
#[derive(Default)]
pub struct RewriteEngine;

impl RewriteEngine {
    pub fn new() -> Self {
        Self
    }

    /// Rewrite a SELECT query, adding helper aggregates when necessary.
    pub fn rewrite_select(
        &self,
        ast: &ParseResult,
        sql: &str,
        aggregate: &Aggregate,
    ) -> RewriteOutput {
        self.rewrite_parsed(ast, aggregate, sql)
    }

    fn rewrite_parsed(
        &self,
        parsed: &ParseResult,
        aggregate: &Aggregate,
        original_sql: &str,
    ) -> RewriteOutput {
        let mut ast = parsed.protobuf.clone();
        let Some(raw_stmt) = ast.stmts.first_mut() else {
            return RewriteOutput::new(original_sql.to_string(), RewritePlan::new());
        };

        let Some(stmt) = raw_stmt.stmt.as_mut() else {
            return RewriteOutput::new(original_sql.to_string(), RewritePlan::new());
        };

        let Some(NodeEnum::SelectStmt(select)) = stmt.node.as_mut() else {
            return RewriteOutput::new(original_sql.to_string(), RewritePlan::new());
        };

        let mut plan = RewritePlan::new();
        let mut modified = false;

        for target in aggregate
            .targets()
            .iter()
            .filter(|t| matches!(t.function(), AggregateFunction::Avg))
        {
            if aggregate.targets().iter().any(|other| {
                matches!(other.function(), AggregateFunction::Count)
                    && other.expr_id() == target.expr_id()
                    && other.is_distinct() == target.is_distinct()
            }) {
                continue;
            }

            let Some(node) = select.target_list.get(target.column()) else {
                continue;
            };
            let Some(NodeEnum::ResTarget(res_target)) = node.node.as_ref() else {
                continue;
            };
            let Some(original_value) = res_target.val.as_ref() else {
                continue;
            };
            let Some(func_call) = Self::extract_func_call(original_value) else {
                continue;
            };

            let helper_index = select.target_list.len();
            let helper_alias = format!("__pgdog_count_{}", helper_index);

            let helper_func = Self::build_count_func(func_call, target.is_distinct());
            let helper_res = ResTarget {
                name: helper_alias,
                indirection: vec![],
                val: Some(Box::new(Node {
                    node: Some(NodeEnum::FuncCall(Box::new(helper_func))),
                })),
                location: res_target.location,
            };

            select.target_list.push(Node {
                node: Some(NodeEnum::ResTarget(Box::new(helper_res))),
            });

            plan.add_drop_column(helper_index);
            plan.add_helper(HelperMapping {
                avg_column: target.column(),
                helper_column: helper_index,
                expr_id: target.expr_id(),
                distinct: target.is_distinct(),
            });

            modified = true;
        }

        if !modified {
            return RewriteOutput::new(original_sql.to_string(), RewritePlan::new());
        }

        let rewritten_sql = ast.deparse().unwrap_or_else(|_| original_sql.to_string());
        RewriteOutput::new(rewritten_sql, plan)
    }

    fn extract_func_call(node: &Node) -> Option<&FuncCall> {
        match node.node.as_ref()? {
            NodeEnum::FuncCall(func) => Some(func),
            NodeEnum::TypeCast(cast) => cast
                .arg
                .as_deref()
                .and_then(|inner| Self::extract_func_call(inner)),
            NodeEnum::CollateClause(collate) => collate
                .arg
                .as_deref()
                .and_then(|inner| Self::extract_func_call(inner)),
            NodeEnum::CoerceToDomain(coerce) => coerce
                .arg
                .as_deref()
                .and_then(|inner| Self::extract_func_call(inner)),
            NodeEnum::ResTarget(res) => res
                .val
                .as_deref()
                .and_then(|inner| Self::extract_func_call(inner)),
            _ => None,
        }
    }

    fn build_count_func(original: &FuncCall, distinct: bool) -> FuncCall {
        FuncCall {
            funcname: vec![Node {
                node: Some(NodeEnum::String(PgString {
                    sval: "count".into(),
                })),
            }],
            args: original.args.clone(),
            agg_order: original.agg_order.clone(),
            agg_filter: original.agg_filter.clone(),
            over: original.over.clone(),
            agg_within_group: original.agg_within_group,
            agg_star: original.agg_star,
            agg_distinct: distinct,
            func_variadic: original.func_variadic,
            funcformat: original.funcformat,
            location: original.location,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::frontend::router::parser::aggregate::Aggregate;

    fn rewrite(sql: &str) -> RewriteOutput {
        let ast = pg_query::parse(sql).unwrap();
        let stmt = match ast
            .protobuf
            .stmts
            .first()
            .and_then(|stmt| stmt.stmt.as_ref())
        {
            Some(raw) => match raw.node.as_ref().unwrap() {
                NodeEnum::SelectStmt(select) => select,
                _ => panic!("not a select"),
            },
            None => panic!("empty"),
        };
        let aggregate = Aggregate::parse(stmt).unwrap();
        RewriteEngine::new().rewrite_select(&ast, sql, &aggregate)
    }

    #[test]
    fn rewrite_engine_noop() {
        let output = rewrite("SELECT COUNT(price) FROM menu");
        assert!(output.plan.is_noop());
    }

    #[test]
    fn rewrite_engine_adds_helper() {
        let output = rewrite("SELECT AVG(price) FROM menu");
        assert!(!output.plan.is_noop());
        assert_eq!(output.plan.drop_columns(), &[1]);
        assert_eq!(output.plan.helpers().len(), 1);
        let helper = &output.plan.helpers()[0];
        assert_eq!(helper.avg_column, 0);
        assert_eq!(helper.helper_column, 1);
        assert_eq!(helper.expr_id, 0);
        assert!(!helper.distinct);

        let parsed = pg_query::parse(&output.sql).unwrap();
        let stmt = match parsed
            .protobuf
            .stmts
            .first()
            .and_then(|stmt| stmt.stmt.as_ref())
        {
            Some(raw) => match raw.node.as_ref().unwrap() {
                NodeEnum::SelectStmt(select) => select,
                _ => panic!("not select"),
            },
            None => panic!("empty"),
        };
        let aggregate = Aggregate::parse(stmt).unwrap();
        assert_eq!(aggregate.targets().len(), 2);
        assert!(aggregate
            .targets()
            .iter()
            .any(|target| matches!(target.function(), AggregateFunction::Count)));
    }

    #[test]
    fn rewrite_engine_handles_avg_with_casts() {
        let output = rewrite("SELECT AVG(amount)::numeric::bigint::real FROM sharded");
        assert_eq!(output.plan.drop_columns(), &[1]);
        assert_eq!(output.plan.helpers().len(), 1);
        let helper = &output.plan.helpers()[0];
        assert_eq!(helper.avg_column, 0);
        assert_eq!(helper.helper_column, 1);

        let parsed = pg_query::parse(&output.sql).unwrap();
        let stmt = match parsed
            .protobuf
            .stmts
            .first()
            .and_then(|stmt| stmt.stmt.as_ref())
        {
            Some(raw) => match raw.node.as_ref().unwrap() {
                NodeEnum::SelectStmt(select) => select,
                _ => panic!("not select"),
            },
            None => panic!("empty"),
        };
        let aggregate = Aggregate::parse(stmt).unwrap();
        assert_eq!(aggregate.targets().len(), 2);
        assert!(aggregate
            .targets()
            .iter()
            .any(|target| matches!(target.function(), AggregateFunction::Count)));
    }

    #[test]
    fn rewrite_engine_skips_when_count_exists() {
        let sql = "SELECT COUNT(price), AVG(price) FROM menu";
        let output = rewrite(sql);
        assert!(output.plan.is_noop());
        assert_eq!(output.sql, sql);
    }

    #[test]
    fn rewrite_engine_handles_mismatched_pair() {
        let output = rewrite("SELECT COUNT(price::numeric), AVG(price) FROM menu");
        assert_eq!(output.plan.drop_columns(), &[2]);
        assert_eq!(output.plan.helpers().len(), 1);
        let helper = &output.plan.helpers()[0];
        assert_eq!(helper.avg_column, 1);
        assert_eq!(helper.helper_column, 2);
        assert!(!helper.distinct);

        // Ensure the rewritten SQL now contains both AVG and helper COUNT for the AVG target.
        let parsed = pg_query::parse(&output.sql).unwrap();
        let stmt = match parsed
            .protobuf
            .stmts
            .first()
            .and_then(|stmt| stmt.stmt.as_ref())
        {
            Some(raw) => match raw.node.as_ref().unwrap() {
                NodeEnum::SelectStmt(select) => select,
                _ => panic!("not select"),
            },
            None => panic!("empty"),
        };
        let aggregate = Aggregate::parse(stmt).unwrap();
        assert_eq!(aggregate.targets().len(), 3);
        assert!(
            aggregate
                .targets()
                .iter()
                .filter(|target| matches!(target.function(), AggregateFunction::Count))
                .count()
                >= 2
        );
    }

    #[test]
    fn rewrite_engine_multiple_avg_helpers() {
        let output = rewrite("SELECT AVG(price), AVG(discount) FROM menu");
        assert_eq!(output.plan.drop_columns(), &[2, 3]);
        assert_eq!(output.plan.helpers().len(), 2);

        let helper_price = &output.plan.helpers()[0];
        assert_eq!(helper_price.avg_column, 0);
        assert_eq!(helper_price.helper_column, 2);

        let helper_discount = &output.plan.helpers()[1];
        assert_eq!(helper_discount.avg_column, 1);
        assert_eq!(helper_discount.helper_column, 3);

        let parsed = pg_query::parse(&output.sql).unwrap();
        let stmt = match parsed
            .protobuf
            .stmts
            .first()
            .and_then(|stmt| stmt.stmt.as_ref())
        {
            Some(raw) => match raw.node.as_ref().unwrap() {
                NodeEnum::SelectStmt(select) => select,
                _ => panic!("not select"),
            },
            None => panic!("empty"),
        };
        let aggregate = Aggregate::parse(stmt).unwrap();
        assert_eq!(aggregate.targets().len(), 4);
        assert_eq!(
            aggregate
                .targets()
                .iter()
                .filter(|target| matches!(target.function(), AggregateFunction::Count))
                .count(),
            2
        );
    }
}
