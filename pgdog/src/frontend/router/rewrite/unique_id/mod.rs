//! Unique ID rewrite engine.

use pg_query::{
    protobuf::{a_const::Val, AConst, Node, ParamRef, ParseResult, TypeCast, TypeName},
    NodeEnum, NodeRef,
};

pub mod explain;
pub mod insert;
pub mod select;
pub mod update;

pub use explain::ExplainUniqueIdRewrite;
pub use insert::InsertUniqueIdRewrite;
pub use select::SelectUniqueIdRewrite;
pub use update::UpdateUniqueIdRewrite;

pub struct UniqueIdRewrite;

/// Create a bigint-typed parameter reference node.
fn bigint_param(number: i32) -> NodeEnum {
    NodeEnum::TypeCast(Box::new(TypeCast {
        arg: Some(Box::new(Node {
            node: Some(NodeEnum::ParamRef(ParamRef {
                number,
                ..Default::default()
            })),
        })),
        type_name: Some(TypeName {
            names: vec![
                Node {
                    node: Some(NodeEnum::String(pg_query::protobuf::String {
                        sval: "pg_catalog".to_string(),
                    })),
                },
                Node {
                    node: Some(NodeEnum::String(pg_query::protobuf::String {
                        sval: "int8".to_string(),
                    })),
                },
            ],
            ..Default::default()
        }),
        ..Default::default()
    }))
}

/// Create a bigint-typed constant node for the given ID.
fn bigint_const(id: i64) -> NodeEnum {
    NodeEnum::TypeCast(Box::new(TypeCast {
        arg: Some(Box::new(Node {
            node: Some(NodeEnum::AConst(AConst {
                val: Some(Val::Sval(pg_query::protobuf::String {
                    sval: id.to_string(),
                })),
                ..Default::default()
            })),
        })),
        type_name: Some(TypeName {
            names: vec![
                Node {
                    node: Some(NodeEnum::String(pg_query::protobuf::String {
                        sval: "pg_catalog".to_string(),
                    })),
                },
                Node {
                    node: Some(NodeEnum::String(pg_query::protobuf::String {
                        sval: "int8".to_string(),
                    })),
                },
            ],
            ..Default::default()
        }),
        ..Default::default()
    }))
}

/// Find the maximum parameter number ($N) in a parse result.
pub fn max_param_number(result: &ParseResult) -> i32 {
    result
        .nodes()
        .iter()
        .filter_map(|(node, _, _, _)| {
            if let NodeRef::ParamRef(p) = node {
                Some(p.number)
            } else {
                None
            }
        })
        .max()
        .unwrap_or(0)
}
