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
    pub fn parse(stmt: &SelectStmt) -> Result<Vec<Self>, Error> {
        let mut aggs = vec![];

        for (idx, node) in stmt.target_list.iter().enumerate() {
            match &node.node {
                Some(NodeEnum::ResTarget(ref res)) => match &res.val {
                    Some(node) => match &node.node {
                        Some(NodeEnum::FuncCall(func)) => {
                            if let Some(name) = func.funcname.first() {
                                if let Some(NodeEnum::String(protobuf::String { sval })) =
                                    &name.node
                                {
                                    match sval.as_str() {
                                        "count" => {
                                            if func.args.is_empty() && stmt.group_clause.is_empty()
                                            {
                                                aggs.push(Aggregate::Count(AggregateTarget::Star(
                                                    idx,
                                                )));
                                            }
                                        }

                                        _ => (),
                                    }
                                }
                            }
                        }

                        _ => (),
                    },

                    _ => (),
                },
                _ => (),
            }
        }

        Ok(aggs)
    }
}
