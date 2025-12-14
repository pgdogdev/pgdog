use crate::frontend::router::parser::aggregate::{Aggregate, AggregateFunction};
use pg_query::protobuf::{
    a_const::Val, AConst, FuncCall, Integer, Node, ParseResult, ResTarget, String as PgString,
};
use pg_query::NodeEnum;

use super::{AggregateRewritePlan, HelperKind, HelperMapping, RewriteOutput};

/// Query rewrite engine. Currently supports injecting helper aggregates for AVG and
/// variance-related functions that require additional helper aggregates when run
/// across shards.
#[derive(Default)]
pub struct AggregatesRewrite;

impl AggregatesRewrite {
    pub fn new() -> Self {
        Self
    }

    /// Rewrite a SELECT query in-place, adding helper aggregates when necessary.
    pub fn rewrite_select(&self, ast: &mut ParseResult, aggregate: &Aggregate) -> RewriteOutput {
        self.rewrite_parsed(ast, aggregate)
    }

    fn rewrite_parsed(&self, parsed: &mut ParseResult, aggregate: &Aggregate) -> RewriteOutput {
        let Some(raw_stmt) = parsed.stmts.first() else {
            return RewriteOutput::default();
        };

        let Some(stmt) = raw_stmt.stmt.as_ref() else {
            return RewriteOutput::default();
        };

        let Some(NodeEnum::SelectStmt(select)) = stmt.node.as_ref() else {
            return RewriteOutput::default();
        };

        let mut plan = AggregateRewritePlan::new();
        let mut helper_nodes: Vec<Node> = Vec::new();
        let mut planned_aliases: Vec<String> = Vec::new();
        let base_len = select.target_list.len();

        for target in aggregate.targets() {
            if aggregate.targets().iter().any(|other| {
                matches!(other.function(), AggregateFunction::Count)
                    && other.expr_id() == target.expr_id()
                    && other.is_distinct() == target.is_distinct()
            }) && matches!(target.function(), AggregateFunction::Avg)
            {
                continue;
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
                let HelperSpec { func, kind } = helper;

                let helper_alias = format!(
                    "__pgdog_{}_expr{}_col{}",
                    kind.alias_suffix(),
                    target.expr_id(),
                    target.column()
                );

                let alias_exists = select.target_list.iter().any(|existing| {
                    if let Some(NodeEnum::ResTarget(res)) = existing.node.as_ref() {
                        res.name == helper_alias
                    } else {
                        false
                    }
                }) || planned_aliases
                    .iter()
                    .any(|existing| existing == &helper_alias);

                if alias_exists {
                    continue;
                }

                let helper_column = base_len + helper_nodes.len();

                let helper_res = ResTarget {
                    name: helper_alias.clone(),
                    indirection: vec![],
                    val: Some(Box::new(Node {
                        node: Some(NodeEnum::FuncCall(Box::new(func))),
                    })),
                    location,
                };

                helper_nodes.push(Node {
                    node: Some(NodeEnum::ResTarget(Box::new(helper_res))),
                });
                planned_aliases.push(helper_alias.clone());

                plan.add_drop_column(helper_column);
                plan.add_helper(HelperMapping {
                    target_column: target.column(),
                    helper_column,
                    expr_id: target.expr_id(),
                    distinct: target.is_distinct(),
                    kind,
                    alias: helper_alias,
                });
            }
        }

        if helper_nodes.is_empty() {
            return RewriteOutput::default();
        }

        let Some(raw_stmt) = parsed.stmts.first_mut() else {
            return RewriteOutput::default();
        };

        let Some(stmt) = raw_stmt.stmt.as_mut() else {
            return RewriteOutput::default();
        };

        let Some(NodeEnum::SelectStmt(select)) = stmt.node.as_mut() else {
            return RewriteOutput::default();
        };

        select.target_list.extend(helper_nodes);

        RewriteOutput::new(plan)
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::frontend::router::parser::aggregate::Aggregate;

    fn select(ast: &ParseResult) -> &pg_query::protobuf::SelectStmt {
        match ast
            .stmts
            .first()
            .and_then(|stmt| stmt.stmt.as_ref())
            .and_then(|stmt| stmt.node.as_ref())
        {
            Some(NodeEnum::SelectStmt(select)) => select,
            _ => panic!("not a select"),
        }
    }

    fn rewrite(sql: &str) -> (ParseResult, RewriteOutput) {
        let mut parsed = pg_query::parse(sql).unwrap().protobuf;
        let aggregate = {
            let stmt = select(&parsed);
            Aggregate::parse(stmt)
        };
        let output = AggregatesRewrite::new().rewrite_select(&mut parsed, &aggregate);
        (parsed, output)
    }

    #[test]
    fn rewrite_engine_noop() {
        let (ast, output) = rewrite("SELECT COUNT(price) FROM menu");
        assert!(output.plan.is_noop());
        assert_eq!(select(&ast).target_list.len(), 1);
    }

    #[test]
    fn rewrite_engine_adds_helper() {
        let (ast, output) = rewrite("SELECT AVG(price) FROM menu");
        assert!(!output.plan.is_noop());
        assert_eq!(output.plan.drop_columns(), &[1]);
        assert_eq!(output.plan.helpers().len(), 1);
        let helper = &output.plan.helpers()[0];
        assert_eq!(helper.target_column, 0);
        assert_eq!(helper.helper_column, 1);
        assert_eq!(helper.expr_id, 0);
        assert!(!helper.distinct);
        assert!(matches!(helper.kind, HelperKind::Count));

        let aggregate = Aggregate::parse(select(&ast));
        assert_eq!(aggregate.targets().len(), 2);
        assert!(aggregate
            .targets()
            .iter()
            .any(|target| matches!(target.function(), AggregateFunction::Count)));
    }

    #[test]
    fn rewrite_engine_skips_when_count_exists() {
        let sql = "SELECT COUNT(price), AVG(price) FROM menu";
        let (ast, output) = rewrite(sql);
        assert!(output.plan.is_noop());
        assert_eq!(select(&ast).target_list.len(), 2);
    }

    #[test]
    fn rewrite_engine_handles_mismatched_pair() {
        let (ast, output) = rewrite("SELECT COUNT(price::numeric), AVG(price) FROM menu");
        assert_eq!(output.plan.drop_columns(), &[2]);
        assert_eq!(output.plan.helpers().len(), 1);
        let helper = &output.plan.helpers()[0];
        assert_eq!(helper.target_column, 1);
        assert_eq!(helper.helper_column, 2);
        assert!(!helper.distinct);
        assert!(matches!(helper.kind, HelperKind::Count));

        let aggregate = Aggregate::parse(select(&ast));
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
        let (ast, output) = rewrite("SELECT AVG(price), AVG(discount) FROM menu");
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

        let aggregate = Aggregate::parse(select(&ast));
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
        let (ast, output) = rewrite("SELECT STDDEV(price) FROM menu");
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

        // Expect original STDDEV plus three helpers.
        assert_eq!(select(&ast).target_list.len(), 4);
    }
}
