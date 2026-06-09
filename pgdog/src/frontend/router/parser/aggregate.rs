use pg_query::NodeEnum;
use pg_query::protobuf::Integer;
use pg_query::protobuf::{Node, SelectStmt, String as PgQueryString, a_const::Val};

use super::{ExpressionRegistry, Function};
use crate::backend::schema::Schema;

#[derive(Debug, Clone, PartialEq)]
pub struct AggregateTarget {
    column: usize,
    function: AggregateFunction,
    expr_id: usize,
    distinct: bool,
}

impl AggregateTarget {
    pub fn function(&self) -> &AggregateFunction {
        &self.function
    }

    pub fn column(&self) -> usize {
        self.column
    }

    pub fn expr_id(&self) -> usize {
        self.expr_id
    }

    pub fn is_distinct(&self) -> bool {
        self.distinct
    }
}

#[derive(Debug, Clone, PartialEq)]
pub enum AggregateFunction {
    Count,
    Max,
    Min,
    Avg,
    Sum,
    StddevPop,
    StddevSamp,
    VarPop,
    VarSamp,
    Unrecognized(String),
}

impl AggregateFunction {
    pub fn as_str(&self) -> &str {
        match self {
            AggregateFunction::Count => "count",
            AggregateFunction::Max => "max",
            AggregateFunction::Min => "min",
            AggregateFunction::Avg => "avg",
            AggregateFunction::Sum => "sum",
            AggregateFunction::StddevPop => "stddev_pop",
            AggregateFunction::StddevSamp => "stddev_samp",
            AggregateFunction::VarPop => "var_pop",
            AggregateFunction::VarSamp => "var_samp",
            AggregateFunction::Unrecognized(s) => &*s,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Default)]
pub struct Aggregate {
    targets: Vec<AggregateTarget>,
    group_by: Vec<usize>,
}

fn target_list_to_index(stmt: &SelectStmt, column_names: Vec<&String>) -> Option<usize> {
    for (idx, node) in stmt.target_list.iter().enumerate() {
        if let Some(NodeEnum::ResTarget(res_target_box)) = node.node.as_ref() {
            let res_target = res_target_box.as_ref();
            if let Some(node_box) = res_target.val.as_ref() {
                if let Some(NodeEnum::ColumnRef(column_ref)) = node_box.node.as_ref() {
                    let select_names: Vec<&String> = column_ref
                        .fields
                        .iter()
                        .filter_map(|field_node| {
                            if let Some(node_box) = field_node.node.as_ref() {
                                match node_box {
                                    NodeEnum::String(PgQueryString {
                                        sval: found_column_name,
                                        ..
                                    }) => Some(found_column_name),
                                    _ => None,
                                }
                            } else {
                                None
                            }
                        })
                        .collect();

                    if select_names.is_empty() {
                        continue;
                    }

                    if columns_match(&column_names, &select_names) {
                        return Some(idx);
                    }
                }
            }
        }
    }
    None
}

fn columns_match(group_by_names: &[&String], select_names: &[&String]) -> bool {
    if group_by_names == select_names {
        return true;
    }

    if group_by_names.len() == 1 && select_names.len() == 2 {
        return select_names[1] == group_by_names[0];
    }

    if group_by_names.len() == 2 && select_names.len() == 1 {
        return group_by_names[1] == select_names[0];
    }

    false
}

impl Aggregate {
    /// Figure out what aggregates are present and which ones PgDog supports.
    pub fn parse(stmt: &SelectStmt, schema: &Schema) -> Self {
        let mut targets = vec![];
        let mut registry = ExpressionRegistry::new();
        let group_by = stmt
            .group_clause
            .iter()
            .filter_map(|node| {
                node.node.as_ref().map(|node| match node {
                    NodeEnum::AConst(aconst) => aconst.val.as_ref().map(|val| match val {
                        Val::Ival(Integer { ival }) => Some(*ival as usize - 1), // We use 0-indexed arrays, Postgres uses 1-indexed.
                        _ => None,
                    }),
                    NodeEnum::ColumnRef(column_ref) => {
                        let column_names: Vec<&String> = column_ref
                            .fields
                            .iter()
                            .filter_map(|node| match node {
                                Node {
                                    node:
                                        Some(NodeEnum::String(PgQueryString { sval: column_name })),
                                } => Some(column_name),
                                _ => None,
                            })
                            .collect();
                        Some(target_list_to_index(stmt, column_names))
                    }
                    _ => None,
                })
            })
            .flatten()
            .flatten()
            .collect::<Vec<_>>();

        for (idx, node) in stmt.target_list.iter().enumerate() {
            if let Some(NodeEnum::ResTarget(res)) = &node.node {
                if let Some(node) = &res.val {
                    if let Ok(func) = Function::try_from(node.as_ref()) {
                        let function = match func.name {
                            "count" => Some(AggregateFunction::Count),
                            "max" => Some(AggregateFunction::Max),
                            "min" => Some(AggregateFunction::Min),
                            "sum" => Some(AggregateFunction::Sum),
                            "avg" => Some(AggregateFunction::Avg),
                            "stddev" | "stddev_samp" => Some(AggregateFunction::StddevSamp),
                            "stddev_pop" => Some(AggregateFunction::StddevPop),
                            "variance" | "var_samp" => Some(AggregateFunction::VarSamp),
                            "var_pop" => Some(AggregateFunction::VarPop),
                            fname => {
                                if schema.aggregate_functions.contains(fname) {
                                    Some(AggregateFunction::Unrecognized(fname.to_owned()))
                                } else {
                                    None
                                }
                            }
                        };

                        if let Some(function) = function {
                            let (expr_id, distinct) = match node.node.as_ref() {
                                Some(NodeEnum::FuncCall(func)) => {
                                    let arg_id = func
                                        .args
                                        .first()
                                        .map(|arg| registry.intern(arg))
                                        .unwrap_or_else(|| registry.intern(node.as_ref()));
                                    (arg_id, func.agg_distinct)
                                }
                                _ => (registry.intern(node.as_ref()), false),
                            };

                            targets.push(AggregateTarget {
                                column: idx,
                                function,
                                expr_id,
                                distinct,
                            });
                        }
                    }
                }
            }
        }

        Self { targets, group_by }
    }

    pub fn targets(&self) -> &[AggregateTarget] {
        &self.targets
    }

    pub fn group_by(&self) -> &[usize] {
        &self.group_by
    }

    pub fn new_count(column: usize) -> Self {
        Self {
            targets: vec![AggregateTarget {
                function: AggregateFunction::Count,
                column,
                expr_id: 0,
                distinct: false,
            }],
            group_by: vec![],
        }
    }

    pub fn new_count_group_by(column: usize, group_by: &[usize]) -> Self {
        Self {
            targets: vec![AggregateTarget {
                function: AggregateFunction::Count,
                column,
                expr_id: 0,
                distinct: false,
            }],
            group_by: group_by.to_vec(),
        }
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    pub fn len(&self) -> usize {
        self.targets.len()
    }
}

#[cfg(test)]
mod test {
    use super::*;

    fn select(stmt: &str) -> SelectStmt {
        let stmt = pg_query::parse(stmt)
            .unwrap()
            .protobuf
            .stmts
            .remove(0)
            .stmt
            .unwrap();
        match stmt.node.unwrap() {
            NodeEnum::SelectStmt(stmt) => *stmt,
            _ => panic!("not a select"),
        }
    }

    fn parse(stmt: &str) -> Aggregate {
        Aggregate::parse(&select(stmt), &Default::default())
    }

    #[test]
    fn test_parse_aggregate() {
        let aggr = parse("SELECT COUNT(*)::bigint FROM users");
        assert_eq!(
            aggr.targets().first().unwrap().function,
            AggregateFunction::Count
        );
    }

    #[test]
    fn test_parse_avg_count_expr_id_matches() {
        let aggr = parse("SELECT COUNT(price), AVG(price) FROM menu");
        assert_eq!(aggr.targets().len(), 2);
        let count = &aggr.targets()[0];
        let avg = &aggr.targets()[1];
        assert!(matches!(count.function(), AggregateFunction::Count));
        assert!(matches!(avg.function(), AggregateFunction::Avg));
        assert_eq!(count.expr_id(), avg.expr_id());
        assert!(!count.is_distinct());
        assert!(!avg.is_distinct());
    }

    #[test]
    fn test_parse_avg_count_expr_id_differs() {
        let aggr = parse("SELECT COUNT(price), AVG(cost) FROM menu");
        assert_eq!(aggr.targets().len(), 2);
        let count = &aggr.targets()[0];
        let avg = &aggr.targets()[1];
        assert!(matches!(count.function(), AggregateFunction::Count));
        assert!(matches!(avg.function(), AggregateFunction::Avg));
        assert_ne!(count.expr_id(), avg.expr_id());
        assert!(!count.is_distinct());
        assert!(!avg.is_distinct());
    }

    #[test]
    fn test_parse_distinct_count_not_matching_avg() {
        let aggr = parse("SELECT COUNT(DISTINCT price), AVG(price) FROM menu");
        assert_eq!(aggr.targets().len(), 2);
        let count = &aggr.targets()[0];
        let avg = &aggr.targets()[1];
        assert!(matches!(count.function(), AggregateFunction::Count));
        assert!(matches!(avg.function(), AggregateFunction::Avg));
        assert!(count.is_distinct());
        assert!(!avg.is_distinct());
        assert_eq!(count.expr_id(), avg.expr_id());
    }

    #[test]
    fn test_parse_stddev_variants() {
        let aggr = parse("SELECT STDDEV(price), STDDEV_SAMP(price), STDDEV_POP(price) FROM menu");
        assert_eq!(aggr.targets().len(), 3);
        assert!(matches!(
            aggr.targets()[0].function(),
            AggregateFunction::StddevSamp
        ));
        assert!(matches!(
            aggr.targets()[1].function(),
            AggregateFunction::StddevSamp
        ));
        assert!(matches!(
            aggr.targets()[2].function(),
            AggregateFunction::StddevPop
        ));
    }

    #[test]
    fn test_parse_variance_variants() {
        let aggr = parse("SELECT VARIANCE(price), VAR_SAMP(price), VAR_POP(price) FROM menu");
        assert_eq!(aggr.targets().len(), 3);
        assert!(matches!(
            aggr.targets()[0].function(),
            AggregateFunction::VarSamp
        ));
        assert!(matches!(
            aggr.targets()[1].function(),
            AggregateFunction::VarSamp
        ));
        assert!(matches!(
            aggr.targets()[2].function(),
            AggregateFunction::VarPop
        ));
    }

    #[test]
    fn test_parse_group_by_ordinals() {
        let aggr = parse("SELECT price, category_id, SUM(quantity) FROM menu GROUP BY 1, 2");
        assert_eq!(aggr.group_by(), &[0, 1]);
        assert_eq!(aggr.targets().len(), 1);
        let target = &aggr.targets()[0];
        assert!(matches!(target.function(), AggregateFunction::Sum));
        assert_eq!(target.column(), 2);
    }

    #[test]
    fn test_parse_sum_distinct_sets_flag() {
        let aggr = parse("SELECT SUM(DISTINCT price) FROM menu");
        assert_eq!(aggr.targets().len(), 1);
        let target = &aggr.targets()[0];
        assert!(matches!(target.function(), AggregateFunction::Sum));
        assert!(target.is_distinct());
    }

    #[test]
    fn test_parse_group_by_column_name_single() {
        let aggr = parse("SELECT user_id, COUNT(1) FROM example GROUP BY user_id");
        assert_eq!(aggr.group_by(), &[0]);
        assert_eq!(aggr.targets().len(), 1);
        let target = &aggr.targets()[0];
        assert!(matches!(target.function(), AggregateFunction::Count));
        assert_eq!(target.column(), 1);
    }

    #[test]
    fn test_parse_group_by_column_name_multiple() {
        let aggr = parse(
            "SELECT COUNT(*), user_id, category_id FROM example GROUP BY user_id, category_id",
        );
        assert_eq!(aggr.group_by(), &[1, 2]);
        assert_eq!(aggr.targets().len(), 1);
        let target = &aggr.targets()[0];
        assert!(matches!(target.function(), AggregateFunction::Count));
        assert_eq!(target.column(), 0);
    }

    #[test]
    fn test_parse_group_by_qualified_column_name() {
        let aggr = parse("SELECT COUNT(1), example.user_id FROM example GROUP BY example.user_id");
        assert_eq!(aggr.group_by(), &[1]);
        assert_eq!(aggr.targets().len(), 1);
        let target = &aggr.targets()[0];
        assert!(matches!(target.function(), AggregateFunction::Count));
        assert_eq!(target.column(), 0);
    }

    #[test]
    fn test_parse_group_by_mixed_ordinal_and_column_name() {
        let aggr =
            parse("SELECT user_id, category_id, SUM(quantity) FROM example GROUP BY user_id, 2");
        assert_eq!(aggr.group_by(), &[0, 1]);
        assert_eq!(aggr.targets().len(), 1);
        let target = &aggr.targets()[0];
        assert!(matches!(target.function(), AggregateFunction::Sum));
        assert_eq!(target.column(), 2);
    }

    #[test]
    fn test_parse_group_by_column_not_in_select() {
        let aggr = parse("SELECT COUNT(*) FROM example GROUP BY user_id");
        assert!(aggr.group_by().is_empty());
        assert_eq!(aggr.targets().len(), 1);
    }

    #[test]
    fn test_parse_group_by_with_multiple_aggregates() {
        let aggr =
            parse("SELECT COUNT(*), SUM(price), user_id, AVG(price) FROM example GROUP BY user_id");
        assert_eq!(aggr.group_by(), &[2]);
        assert_eq!(aggr.targets().len(), 3);
        assert!(matches!(
            aggr.targets()[0].function(),
            AggregateFunction::Count
        ));
        assert!(matches!(
            aggr.targets()[1].function(),
            AggregateFunction::Sum
        ));
        assert!(matches!(
            aggr.targets()[2].function(),
            AggregateFunction::Avg
        ));
    }

    #[test]
    fn test_parse_group_by_qualified_matches_select_unqualified() {
        let aggr = parse("SELECT user_id, COUNT(1) FROM example GROUP BY example.user_id");
        assert_eq!(aggr.group_by(), &[0]);
        assert_eq!(aggr.targets().len(), 1);
    }

    #[test]
    fn test_parse_group_by_unqualified_matches_select_qualified() {
        let aggr = parse("SELECT example.user_id, COUNT(1) FROM example GROUP BY user_id");
        assert_eq!(aggr.group_by(), &[0]);
        assert_eq!(aggr.targets().len(), 1);
    }

    #[test]
    fn test_parse_group_by_both_qualified_order_matters() {
        let aggr = parse("SELECT example.user_id, COUNT(1) FROM example GROUP BY example.user_id");
        assert_eq!(aggr.group_by(), &[0]);
        assert_eq!(aggr.targets().len(), 1);
    }

    #[test]
    fn test_unrecognized_aggregate_function_errors() {
        let schema_with_agg = Schema::from_parts_with_agg(
            Vec::new(),
            Default::default(),
            vec![String::from("mysum")],
        );
        let schema_without_agg = Default::default();
        let query = select("SELECT mysum(lol) FROM example");

        // A random function that isn't listed as aggregate in the schema
        // doesn't require special support on our end, so we should be fine.
        let aggregate = Aggregate::parse(&query, &schema_without_agg);
        assert_eq!(aggregate.targets, Vec::new());

        // If we see an aggregate function we don't recognize, we can't
        // process the query correctly, since we need to combine the
        // results from each shard.
        let aggregate = Aggregate::parse(&query, &schema_with_agg);
        let funcs = aggregate
            .targets
            .into_iter()
            .map(|t| t.function)
            .collect::<Vec<_>>();
        assert_eq!(
            funcs,
            vec![AggregateFunction::Unrecognized("mysum".to_owned())]
        );
    }
}
