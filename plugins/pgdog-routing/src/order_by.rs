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
        if let Some(NodeEnum::SortBy(ref sort_by)) = clause.node {
            let asc = matches!(sort_by.sortby_dir, 0..=2);
            if let Some(ref node) = sort_by.node {
                if let Some(ref node) = node.node {
                    match node {
                        NodeEnum::AConst(aconst) => {
                            if let Some(Val::Ival(ref integer)) = aconst.val {
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

                        NodeEnum::ColumnRef(column_ref) => {
                            if let Some(field) = column_ref.fields.first() {
                                if let Some(NodeEnum::String(ref string)) = field.node {
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
                        _ => (),
                    }
                }
            }
        }
    }
    Ok(order_by)
}
