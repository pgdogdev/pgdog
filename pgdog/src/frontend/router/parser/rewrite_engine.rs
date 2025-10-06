use super::{Aggregate, AggregateFunction, HelperKind, HelperMapping, RewriteOutput, RewritePlan};
use pg_query::protobuf::{
    a_const::Val, AConst, FuncCall, Integer, Node, ResTarget, String as PgString,
};
use pg_query::{NodeEnum, ParseResult};

/// Query rewrite engine. Currently supports injecting helper aggregates for AVG and
/// variance-related functions that require additional helper aggregates when run
/// across shards.
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

        for target in aggregate.targets() {
            if aggregate.targets().iter().any(|other| {
                matches!(other.function(), AggregateFunction::Count)
                    && other.expr_id() == target.expr_id()
                    && other.is_distinct() == target.is_distinct()
            }) {
                if matches!(target.function(), AggregateFunction::Avg) {
                    continue;
                }
            }

            let Some(node) = select.target_list.get(target.column()) else {
                continue;
            };
            let Some((location, helper_specs)) = ({
                if let Some(NodeEnum::ResTarget(res_target)) = node.node.as_ref() {
                    if let Some(original_value) = res_target.val.as_ref() {
                        if let Some(func_call) = Self::extract_func_call(original_value) {
                            let specs = Self::helper_specs(
                                func_call,
                                target.function(),
                                target.is_distinct(),
                            );
                            Some((res_target.location, specs))
                        } else {
                            None
                        }
                    } else {
                        None
                    }
                } else {
                    None
                }
            }) else {
                continue;
            };

            if helper_specs.is_empty() {
                continue;
            }

            for helper in helper_specs {
                let helper_index = select.target_list.len();
                let helper_alias = format!(
                    "__pgdog_{}_expr{}_col{}",
                    helper.kind.alias_suffix(),
                    target.expr_id(),
                    target.column()
                );

                let alias_exists = select.target_list.iter().any(|existing| {
                    if let Some(NodeEnum::ResTarget(res)) = existing.node.as_ref() {
                        res.name == helper_alias
                    } else {
                        false
                    }
                });

                if alias_exists {
                    continue;
                }

                let helper_res = ResTarget {
                    name: helper_alias.clone(),
                    indirection: vec![],
                    val: Some(Box::new(Node {
                        node: Some(NodeEnum::FuncCall(Box::new(helper.func))),
                    })),
                    location,
                };

                select.target_list.push(Node {
                    node: Some(NodeEnum::ResTarget(Box::new(helper_res))),
                });

                plan.add_drop_column(helper_index);
                plan.add_helper(HelperMapping {
                    target_column: target.column(),
                    helper_column: helper_index,
                    expr_id: target.expr_id(),
                    distinct: target.is_distinct(),
                    kind: helper.kind,
                    alias: helper_alias,
                });

                modified = true;
            }
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

    fn build_sum_func(original: &FuncCall, distinct: bool) -> FuncCall {
        FuncCall {
            funcname: vec![Node {
                node: Some(NodeEnum::String(PgString { sval: "sum".into() })),
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

    fn build_sum_of_squares_func(original: &FuncCall, distinct: bool) -> Option<FuncCall> {
        let arg = original.args.first()?.clone();

        let two = Node {
            node: Some(NodeEnum::AConst(AConst {
                val: Some(Val::Ival(Integer { ival: 2 })),
                location: original.location,
                isnull: false,
            })),
        };

        let power = FuncCall {
            funcname: vec![Node {
                node: Some(NodeEnum::String(PgString {
                    sval: "power".into(),
                })),
            }],
            args: vec![arg, two],
            agg_order: vec![],
            agg_filter: None,
            over: None,
            agg_within_group: false,
            agg_star: false,
            agg_distinct: false,
            func_variadic: false,
            funcformat: original.funcformat,
            location: original.location,
        };

        Some(FuncCall {
            funcname: vec![Node {
                node: Some(NodeEnum::String(PgString { sval: "sum".into() })),
            }],
            args: vec![Node {
                node: Some(NodeEnum::FuncCall(Box::new(power))),
            }],
            agg_order: original.agg_order.clone(),
            agg_filter: original.agg_filter.clone(),
            over: original.over.clone(),
            agg_within_group: original.agg_within_group,
            agg_star: false,
            agg_distinct: distinct,
            func_variadic: original.func_variadic,
            funcformat: original.funcformat,
            location: original.location,
        })
    }

    fn helper_specs(
        func_call: &FuncCall,
        function: &AggregateFunction,
        distinct: bool,
    ) -> Vec<HelperSpec> {
        match function {
            AggregateFunction::Avg => vec![HelperSpec {
                func: Self::build_count_func(func_call, distinct),
                kind: HelperKind::Count,
            }],
            AggregateFunction::StddevSamp
            | AggregateFunction::StddevPop
            | AggregateFunction::VarSamp
            | AggregateFunction::VarPop => {
                let mut helpers = vec![
                    HelperSpec {
                        func: Self::build_count_func(func_call, distinct),
                        kind: HelperKind::Count,
                    },
                    HelperSpec {
                        func: Self::build_sum_func(func_call, distinct),
                        kind: HelperKind::Sum,
                    },
                ];

                if let Some(sum_sq) = Self::build_sum_of_squares_func(func_call, distinct) {
                    helpers.push(HelperSpec {
                        func: sum_sq,
                        kind: HelperKind::SumSquares,
                    });
                }

                helpers
            }
            _ => vec![],
        }
    }
}

#[derive(Debug, Clone)]
struct HelperSpec {
    func: FuncCall,
    kind: HelperKind,
}

impl HelperKind {
    fn alias_suffix(&self) -> &'static str {
        match self {
            HelperKind::Count => "count",
            HelperKind::Sum => "sum",
            HelperKind::SumSquares => "sumsq",
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
        assert_eq!(helper.target_column, 0);
        assert_eq!(helper.helper_column, 1);
        assert_eq!(helper.expr_id, 0);
        assert!(!helper.distinct);
        assert!(matches!(helper.kind, HelperKind::Count));

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
        assert_eq!(helper.target_column, 1);
        assert_eq!(helper.helper_column, 2);
        assert!(!helper.distinct);
        assert!(matches!(helper.kind, HelperKind::Count));

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
        assert_eq!(helper_price.target_column, 0);
        assert_eq!(helper_price.helper_column, 2);
        assert!(matches!(helper_price.kind, HelperKind::Count));

        let helper_discount = &output.plan.helpers()[1];
        assert_eq!(helper_discount.target_column, 1);
        assert_eq!(helper_discount.helper_column, 3);
        assert!(matches!(helper_discount.kind, HelperKind::Count));

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

    #[test]
    fn rewrite_engine_stddev_helpers() {
        let output = rewrite("SELECT STDDEV(price) FROM menu");
        assert!(!output.plan.is_noop());
        assert_eq!(output.plan.drop_columns(), &[1, 2, 3]);
        assert_eq!(output.plan.helpers().len(), 3);

        let kinds: Vec<HelperKind> = output
            .plan
            .helpers()
            .iter()
            .map(|helper| {
                assert_eq!(helper.target_column, 0);
                helper.kind
            })
            .collect();

        assert!(kinds.contains(&HelperKind::Count));
        assert!(kinds.contains(&HelperKind::Sum));
        assert!(kinds.contains(&HelperKind::SumSquares));

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
        // Expect original STDDEV plus three helpers.
        assert_eq!(stmt.target_list.len(), 4);
    }
}
