use crate::frontend::router::parser::Function;
use crate::frontend::router::parser::aggregate::{Aggregate, AggregateFunction};
use itertools::*;
use pg_raw_parse::{Node, make, nodes};

use super::{AggregateRewritePlan, HelperKind, HelperMapping, RewriteOutput};

/// Query rewrite engine. Currently supports injecting helper aggregates for AVG and
/// variance-related functions that require additional helper aggregates when run
/// across shards.
#[derive(Default)]
pub(crate) struct AggregatesRewrite;

impl AggregatesRewrite {
    /// Rewrite a SELECT query in-place, adding helper aggregates when necessary.
    pub(crate) fn rewrite_select<'a>(
        select: &mut nodes::SelectStmtMut<'a, '_>,
        mem: make::MemoryToken<'a>,
        aggregate: &Aggregate,
    ) -> RewriteOutput {
        let mut plan = AggregateRewritePlan::new();

        let helper_nodes = aggregate
            .targets()
            .iter()
            .flat_map(|target| {
                let Some(node) = select.target_list().iter().nth(target.column()) else {
                    // FIXME: We should make this impossible statically
                    panic!("SelectStmt did not contain previously parsed column");
                };

                let Some(func_call) = Function::extract_func_call(node.val()) else {
                    panic!("Previously parsed function was not a FuncCall");
                };

                Self::helper_specs(func_call, target.function(), mem)
                    .into_iter()
                    .map(move |spec| (target, spec))
            })
            .enumerate()
            .map(|(idx, (target, HelperSpec { func, kind }))| {
                let helper_alias =
                    format!("__pgdog_{}_col{}", kind.alias_suffix(), target.column());
                let node = mem.make_res_target(Some(&helper_alias), mem.empty(), func.uncast());

                plan.add_helper(HelperMapping {
                    target_column: target.column(),
                    helper_column: select.target_list().len() + idx,
                    distinct: target.is_distinct(),
                    kind,
                    alias: helper_alias,
                });
                node
            })
            .collect::<Vec<_>>();

        if helper_nodes.is_empty() {
            RewriteOutput::default()
        } else {
            select
                .target_list_mut()
                .extend(mem, mem.make_list(&helper_nodes));
            RewriteOutput::new(plan)
        }
    }

    fn build_sum_of_squares_func<'a>(
        original: &nodes::FuncCall,
        mem: make::MemoryToken<'a>,
    ) -> make::Unique<'a, &'a nodes::FuncCall> {
        let mut sumsq = Self::copy_and_rename_function(original, "sum", mem);
        let arg = sumsq
            .args()
            .iter()
            .exactly_one()
            .expect("Aggregate functions should have exactly one argument");

        // We need to cast to NUMERIC to avoid overflow
        let mut numeric_type = mem.make_node::<nodes::TypeName>();
        numeric_type.as_mut().set_names(mem.make_list(&[
            mem.make_string(Some("pg_catalog")),
            mem.make_string(Some("numeric")),
        ]));
        numeric_type.as_mut().set_type_oid(1700);

        let mut cast_arg = mem.make_node::<nodes::TypeCast>();
        cast_arg.as_mut().set_type_name(numeric_type.as_option());
        cast_arg.as_mut().set_arg(mem.make_unique(arg));

        let arg_squared = mem.make_a_expr(
            nodes::A_Expr_Kind::AEXPR_OP,
            mem.make_list(&[mem.make_string(Some("*")).uncast()]),
            mem.make_unique(Node::from(cast_arg.as_ref())),
            cast_arg.uncast(),
        );
        sumsq
            .as_mut()
            .set_args(mem.make_list(&[arg_squared.uncast()]));
        sumsq
    }

    fn helper_specs<'a>(
        func_call: &nodes::FuncCall,
        function: &AggregateFunction,
        mem: make::MemoryToken<'a>,
    ) -> Vec<HelperSpec<'a>> {
        match function {
            AggregateFunction::Avg => {
                vec![HelperSpec {
                    func: Self::copy_and_rename_function(func_call, "count", mem),
                    kind: HelperKind::Count,
                }]
            }
            AggregateFunction::StddevSamp
            | AggregateFunction::StddevPop
            | AggregateFunction::VarSamp
            | AggregateFunction::VarPop => {
                vec![
                    HelperSpec {
                        func: Self::copy_and_rename_function(func_call, "count", mem),
                        kind: HelperKind::Count,
                    },
                    HelperSpec {
                        func: Self::copy_and_rename_function(func_call, "sum", mem),
                        kind: HelperKind::Sum,
                    },
                    HelperSpec {
                        func: Self::build_sum_of_squares_func(func_call, mem),
                        kind: HelperKind::SumSquares,
                    },
                ]
            }
            // Computable from the original aggregate alone
            AggregateFunction::Count
            | AggregateFunction::Sum
            | AggregateFunction::Max
            | AggregateFunction::Min => Vec::new(),
            // We'll error later
            AggregateFunction::Unrecognized(_) => Vec::new(),
        }
    }

    fn copy_and_rename_function<'a>(
        func_call: &nodes::FuncCall,
        name: &str,
        mem: make::MemoryToken<'a>,
    ) -> make::Unique<'a, &'a nodes::FuncCall> {
        let mut func = mem.make_unique(func_call);
        func.as_mut()
            .set_funcname(mem.make_list(&[mem.make_string(Some(name)).uncast()]));
        func
    }
}

struct HelperSpec<'a> {
    func: make::Unique<'a, &'a nodes::FuncCall>,
    kind: HelperKind,
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::frontend::router::parser::aggregate::Aggregate;
    use pg_raw_parse::{Node, Owned, make, nodes};

    fn rewrite(sql: &str) -> (Owned<nodes::SelectStmt>, RewriteOutput) {
        let ast = pg_raw_parse::parse(sql).unwrap();

        let mut output = None;
        let select = make::owned(|mem| {
            let Node::SelectStmt(stmt) = ast.stmts().next().unwrap() else {
                unreachable!("not a select");
            };
            let mut stmt = mem.make_unique(stmt);

            let aggregate = Aggregate::parse(&stmt, &Default::default());
            output = Some(AggregatesRewrite::rewrite_select(
                &mut stmt.as_mut(),
                mem,
                &aggregate,
            ));
            stmt
        });
        (select, output.unwrap())
    }

    #[test]
    fn rewrite_engine_noop() {
        let (ast, output) = rewrite("SELECT COUNT(price) FROM menu");
        assert!(output.plan.is_noop());
        assert_eq!(ast.target_list().len(), 1);
    }

    #[test]
    fn rewrite_engine_adds_helper() {
        let (ast, output) = rewrite("SELECT AVG(price) FROM menu");
        assert!(!output.plan.is_noop());
        assert_eq!(output.plan.drop_columns().collect::<Vec<_>>(), &[1]);
        assert_eq!(output.plan.helpers().len(), 1);
        let helper = &output.plan.helpers()[0];
        assert_eq!(helper.target_column, 0);
        assert_eq!(helper.helper_column, 1);
        assert!(!helper.distinct);
        assert!(matches!(helper.kind, HelperKind::Count));

        let aggregate = Aggregate::parse(&ast, &Default::default());
        assert_eq!(aggregate.targets().len(), 2);
        assert!(
            aggregate
                .targets()
                .iter()
                .any(|target| matches!(target.function(), AggregateFunction::Count))
        );
    }

    #[test]
    fn rewrite_engine_handles_mismatched_pair() {
        let (ast, output) = rewrite("SELECT COUNT(price::numeric), AVG(price) FROM menu");
        assert_eq!(output.plan.drop_columns().collect::<Vec<_>>(), &[2]);
        assert_eq!(output.plan.helpers().len(), 1);
        let helper = &output.plan.helpers()[0];
        assert_eq!(helper.target_column, 1);
        assert_eq!(helper.helper_column, 2);
        assert!(!helper.distinct);
        assert!(matches!(helper.kind, HelperKind::Count));

        let aggregate = Aggregate::parse(&ast, &Default::default());
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
        assert_eq!(output.plan.drop_columns().collect::<Vec<_>>(), &[2, 3]);
        assert_eq!(output.plan.helpers().len(), 2);

        let helper_price = &output.plan.helpers()[0];
        assert_eq!(helper_price.target_column, 0);
        assert_eq!(helper_price.helper_column, 2);
        assert!(matches!(helper_price.kind, HelperKind::Count));

        let helper_discount = &output.plan.helpers()[1];
        assert_eq!(helper_discount.target_column, 1);
        assert_eq!(helper_discount.helper_column, 3);
        assert!(matches!(helper_discount.kind, HelperKind::Count));

        let aggregate = Aggregate::parse(&ast, &Default::default());
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
        assert_eq!(output.plan.drop_columns().collect::<Vec<_>>(), &[1, 2, 3]);
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
        assert_eq!(ast.target_list().len(), 4);
    }
}
