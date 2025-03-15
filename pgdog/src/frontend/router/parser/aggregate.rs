use pg_query::protobuf::{self, SelectStmt};
use pg_query::NodeEnum;

use super::Error;

#[derive(Debug, Clone, PartialEq)]
pub enum AggregateTarget {
    Column(String, usize),
    Star(usize),
}

#[derive(Debug, Clone, PartialEq)]
pub enum Aggregate {
    Count(AggregateTarget),
    Max(AggregateTarget),
    Min(AggregateTarget),
    Avg(AggregateTarget),
}

impl Aggregate {
    /// Figure out what aggregates are present and which ones PgDog supports.
    pub fn parse(stmt: &SelectStmt) -> Result<Vec<Self>, Error> {
        let mut aggs = vec![];

        for (idx, node) in stmt.target_list.iter().enumerate() {
            if let Some(NodeEnum::ResTarget(ref res)) = &node.node {
                if let Some(node) = &res.val {
                    if let Some(NodeEnum::FuncCall(func)) = &node.node {
                        if let Some(name) = func.funcname.first() {
                            if let Some(NodeEnum::String(protobuf::String { sval })) = &name.node {
                                match sval.as_str() {
                                    "count" => {
                                        if stmt.group_clause.is_empty() {
                                            aggs.push(Aggregate::Count(AggregateTarget::Star(idx)));
                                        }
                                    }

                                    "max" => {
                                        if stmt.group_clause.is_empty() {
                                            aggs.push(Aggregate::Max(AggregateTarget::Star(idx)));
                                        }
                                    }

                                    "min" => {
                                        if stmt.group_clause.is_empty() {
                                            aggs.push(Aggregate::Min(AggregateTarget::Star(idx)));
                                        }
                                    }

                                    _ => {}
                                }
                            }
                        }
                    }
                }
            }
        }

        Ok(aggs)
    }
}
