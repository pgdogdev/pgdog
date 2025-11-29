//! Unique ID rewrite engine.

use pg_query::{
    protobuf::{a_const::Val, AConst, Node, TypeCast, TypeName},
    NodeEnum,
};

pub mod insert;
pub mod select;
pub mod update;

pub struct UniqueIdRewrite;

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
