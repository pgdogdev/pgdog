use pg_query::protobuf::Integer;
use pg_query::protobuf::{a_const::Val, Node, SelectStmt, String as PgQueryString};
use pg_query::NodeEnum;
use std::collections::HashSet;

use crate::frontend::router::parser::{ExpressionRegistry, Function};

use super::Error;

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
}

impl AggregateFunction {
    pub fn as_str(&self) -> &'static str {
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
        }
    }
}

#[derive(Debug, Clone, PartialEq, Default)]
pub struct Aggregate {
    targets: Vec<AggregateTarget>,
    group_by: Vec<usize>,
}

// finds a column name in the target list and returns the index.
fn target_list_to_index(stmt: &SelectStmt, column_names: HashSet<&String>) -> Option<usize> {
    for (idx, node) in stmt.target_list.iter().enumerate() {
        if let Some(node) = node.node.as_ref() {
            match node {
                NodeEnum::ResTarget(res_target_box) => {
                    let res_target = res_target_box.as_ref();
                    if let Some(node_box) = res_target.val.as_ref() {
                        if let Some(inner_enum) = node_box.node.as_ref() {
                            match inner_enum {
                                NodeEnum::ColumnRef(column_ref) => {
                                    if column_ref.fields.iter().all(|field_node| {
                                        if let Some(node_box) = field_node.node.as_ref() {
                                            match node_box {
                                                // NOTE: if we have example.user_id, there are two things here, we expect them to match exactly
                                                // this is more restrictive than actual postgres, where you can have
                                                // the group by say user_id, and the select say table_name.user_id
                                                // I don't know if we can do better than this without looking at the structure of the schema.
                                                // I'm also technically not checking the order of these here, where I should be
                                                // but this is quick n' dirty.
                                                NodeEnum::String(PgQueryString {
                                                    sval: found_column_name,
                                                    ..
                                                }) => column_names.contains(found_column_name),
                                                _ => false,
                                            }
                                        } else {
                                            false
                                        }
                                    }) {
                                        return Some(idx);
                                    }
                                }
                                _ => {}
                            }
                        }
                    }
                }
                _ => {}
            }
        }
    }
    return None;
}

impl Aggregate {
    /// Figure out what aggregates are present and which ones PgDog supports.
    pub fn parse(stmt: &SelectStmt) -> Result<Self, Error> {
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
                        let column_names = column_ref
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
            if let Some(NodeEnum::ResTarget(ref res)) = &node.node {
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
                            _ => None,
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

        Ok(Self { targets, group_by })
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

    #[test]
    fn test_parse_aggregate() {
        let query = pg_query::parse("SELECT COUNT(*)::bigint FROM users")
            .unwrap()
            .protobuf
            .stmts
            .first()
            .cloned()
            .unwrap();
        match query.stmt.unwrap().node.unwrap() {
            NodeEnum::SelectStmt(stmt) => {
                let aggr = Aggregate::parse(&stmt).unwrap();
                assert_eq!(
                    aggr.targets().first().unwrap().function,
                    AggregateFunction::Count
                );
            }

            _ => panic!("not a select"),
        }
    }

    #[test]
    fn test_parse_avg_count_expr_id_matches() {
        let query = pg_query::parse("SELECT COUNT(price), AVG(price) FROM menu")
            .unwrap()
            .protobuf
            .stmts
            .first()
            .cloned()
            .unwrap();
        match query.stmt.unwrap().node.unwrap() {
            NodeEnum::SelectStmt(stmt) => {
                let aggr = Aggregate::parse(&stmt).unwrap();
                assert_eq!(aggr.targets().len(), 2);
                let count = &aggr.targets()[0];
                let avg = &aggr.targets()[1];
                assert!(matches!(count.function(), AggregateFunction::Count));
                assert!(matches!(avg.function(), AggregateFunction::Avg));
                assert_eq!(count.expr_id(), avg.expr_id());
                assert!(!count.is_distinct());
                assert!(!avg.is_distinct());
            }
            _ => panic!("not a select"),
        }
    }

    #[test]
    fn test_parse_avg_count_expr_id_differs() {
        let query = pg_query::parse("SELECT COUNT(price), AVG(cost) FROM menu")
            .unwrap()
            .protobuf
            .stmts
            .first()
            .cloned()
            .unwrap();
        match query.stmt.unwrap().node.unwrap() {
            NodeEnum::SelectStmt(stmt) => {
                let aggr = Aggregate::parse(&stmt).unwrap();
                assert_eq!(aggr.targets().len(), 2);
                let count = &aggr.targets()[0];
                let avg = &aggr.targets()[1];
                assert!(matches!(count.function(), AggregateFunction::Count));
                assert!(matches!(avg.function(), AggregateFunction::Avg));
                assert_ne!(count.expr_id(), avg.expr_id());
                assert!(!count.is_distinct());
                assert!(!avg.is_distinct());
            }
            _ => panic!("not a select"),
        }
    }

    #[test]
    fn test_parse_distinct_count_not_matching_avg() {
        let query = pg_query::parse("SELECT COUNT(DISTINCT price), AVG(price) FROM menu")
            .unwrap()
            .protobuf
            .stmts
            .first()
            .cloned()
            .unwrap();
        match query.stmt.unwrap().node.unwrap() {
            NodeEnum::SelectStmt(stmt) => {
                let aggr = Aggregate::parse(&stmt).unwrap();
                assert_eq!(aggr.targets().len(), 2);
                let count = &aggr.targets()[0];
                let avg = &aggr.targets()[1];
                assert!(matches!(count.function(), AggregateFunction::Count));
                assert!(matches!(avg.function(), AggregateFunction::Avg));
                assert!(count.is_distinct());
                assert!(!avg.is_distinct());
                assert_eq!(count.expr_id(), avg.expr_id());
            }
            _ => panic!("not a select"),
        }
    }

    #[test]
    fn test_parse_stddev_variants() {
        let query = pg_query::parse(
            "SELECT STDDEV(price), STDDEV_SAMP(price), STDDEV_POP(price) FROM menu",
        )
        .unwrap()
        .protobuf
        .stmts
        .first()
        .cloned()
        .unwrap();
        match query.stmt.unwrap().node.unwrap() {
            NodeEnum::SelectStmt(stmt) => {
                let aggr = Aggregate::parse(&stmt).unwrap();
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
            _ => panic!("not a select"),
        }
    }

    #[test]
    fn test_parse_variance_variants() {
        let query =
            pg_query::parse("SELECT VARIANCE(price), VAR_SAMP(price), VAR_POP(price) FROM menu")
                .unwrap()
                .protobuf
                .stmts
                .first()
                .cloned()
                .unwrap();
        match query.stmt.unwrap().node.unwrap() {
            NodeEnum::SelectStmt(stmt) => {
                let aggr = Aggregate::parse(&stmt).unwrap();
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
            _ => panic!("not a select"),
        }
    }

    #[test]
    fn test_parse_group_by_ordinals() {
        let query =
            pg_query::parse("SELECT price, category_id, SUM(quantity) FROM menu GROUP BY 1, 2")
                .unwrap()
                .protobuf
                .stmts
                .first()
                .cloned()
                .unwrap();
        match query.stmt.unwrap().node.unwrap() {
            NodeEnum::SelectStmt(stmt) => {
                let aggr = Aggregate::parse(&stmt).unwrap();
                assert_eq!(aggr.group_by(), &[0, 1]);
                assert_eq!(aggr.targets().len(), 1);
                let target = &aggr.targets()[0];
                assert!(matches!(target.function(), AggregateFunction::Sum));
                assert_eq!(target.column(), 2);
            }
            _ => panic!("not a select"),
        }
    }

    #[test]
    fn test_parse_sum_distinct_sets_flag() {
        let query = pg_query::parse("SELECT SUM(DISTINCT price) FROM menu")
            .unwrap()
            .protobuf
            .stmts
            .first()
            .cloned()
            .unwrap();
        match query.stmt.unwrap().node.unwrap() {
            NodeEnum::SelectStmt(stmt) => {
                let aggr = Aggregate::parse(&stmt).unwrap();
                assert_eq!(aggr.targets().len(), 1);
                let target = &aggr.targets()[0];
                assert!(matches!(target.function(), AggregateFunction::Sum));
                assert!(target.is_distinct());
            }
            _ => panic!("not a select"),
        }
    }
}
