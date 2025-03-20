use pg_query::protobuf::Integer;
use pg_query::protobuf::{self, a_const::Val, SelectStmt};
use pg_query::NodeEnum;

use super::Error;

#[derive(Debug, Clone, PartialEq)]
pub struct AggregateTarget {
    column: usize,
    function: AggregateFunction,
}

impl AggregateTarget {
    pub fn function(&self) -> &AggregateFunction {
        &self.function
    }

    pub fn column(&self) -> usize {
        self.column
    }
}

#[derive(Debug, Clone, PartialEq)]
pub enum AggregateFunction {
    Count,
    Max,
    Min,
    Avg,
    Sum,
}

#[derive(Debug, Clone, PartialEq, Default)]
pub struct Aggregate {
    targets: Vec<AggregateTarget>,
    group_by: Vec<usize>,
}

impl Aggregate {
    /// Figure out what aggregates are present and which ones PgDog supports.
    pub fn parse(stmt: &SelectStmt) -> Result<Self, Error> {
        let mut targets = vec![];
        let group_by = stmt
            .group_clause
            .iter()
            .filter_map(|node| {
                node.node.as_ref().map(|node| match node {
                    NodeEnum::AConst(aconst) => aconst.val.as_ref().map(|val| match val {
                        Val::Ival(Integer { ival }) => Some(*ival as usize - 1), // We use 0-indexed arrays, Postgres uses 1-indexed.
                        _ => None,
                    }),
                    _ => None,
                })
            })
            .flatten()
            .flatten()
            .collect::<Vec<_>>();

        for (idx, node) in stmt.target_list.iter().enumerate() {
            if let Some(NodeEnum::ResTarget(ref res)) = &node.node {
                if let Some(node) = &res.val {
                    if let Some(NodeEnum::FuncCall(func)) = &node.node {
                        if let Some(name) = func.funcname.first() {
                            if let Some(NodeEnum::String(protobuf::String { sval })) = &name.node {
                                match sval.as_str() {
                                    "count" => {
                                        targets.push(AggregateTarget {
                                            column: idx,
                                            function: AggregateFunction::Count,
                                        });
                                    }

                                    "max" => {
                                        targets.push(AggregateTarget {
                                            column: idx,
                                            function: AggregateFunction::Max,
                                        });
                                    }

                                    "min" => {
                                        targets.push(AggregateTarget {
                                            column: idx,
                                            function: AggregateFunction::Min,
                                        });
                                    }

                                    "sum" => targets.push(AggregateTarget {
                                        column: idx,
                                        function: AggregateFunction::Max,
                                    }),

                                    _ => {}
                                }
                            }
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
            }],
            group_by: vec![],
        }
    }

    pub fn new_count_group_by(column: usize, group_by: &[usize]) -> Self {
        Self {
            targets: vec![AggregateTarget {
                function: AggregateFunction::Count,
                column,
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
