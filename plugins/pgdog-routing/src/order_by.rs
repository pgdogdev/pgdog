//! Handle the ORDER BY clause.

use pg_query::{
    protobuf::{a_const::*, *},
    Error, NodeEnum,
};
use pgdog_plugin::*;

/// Extract sorting columns.
///
/// If a query spans multiple shards, this allows pgDog to apply
/// sorting rules Postgres used and show the rows in the correct order.
///
pub fn extract(stmt: &SelectStmt) -> Result<Vec<OrderBy>, Error> {
    let mut order_by = vec![];
    for clause in &stmt.sort_clause {
        if let Some(ref node) = clause.node {
            if let NodeEnum::SortBy(sort_by) = node {
                let asc = match sort_by.sortby_dir {
                    0..=2 => true,
                    _ => false,
                };
                if let Some(ref node) = sort_by.node {
                    if let Some(ref node) = node.node {
                        match node {
                            NodeEnum::AConst(aconst) => {
                                if let Some(ref val) = aconst.val {
                                    if let Val::Ival(integer) = val {
                                        order_by.push(OrderBy::column_index(
                                            integer.ival as usize,
                                            if asc {
                                                OrderByDirection_ASCENDING
                                            } else {
                                                OrderByDirection_DESCENDING
                                            },
                                        ));
                                    }
                                }
                            }

                            NodeEnum::ColumnRef(column_ref) => {
                                if let Some(field) = column_ref.fields.first() {
                                    if let Some(ref node) = field.node {
                                        if let NodeEnum::String(string) = node {
                                            order_by.push(OrderBy::column_name(
                                                &string.sval,
                                                if asc {
                                                    OrderByDirection_ASCENDING
                                                } else {
                                                    OrderByDirection_DESCENDING
                                                },
                                            ));
                                        }
                                    }
                                }
                            }
                            _ => (),
                        }
                    }
                }
            }
        }
    }
    Ok(order_by)
}
