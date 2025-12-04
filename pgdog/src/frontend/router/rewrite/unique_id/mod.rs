//! Unique ID rewrite engine.

use pg_query::{
    protobuf::{a_const::Val, AConst, Node, ParamRef, ParseResult, TypeCast, TypeName},
    NodeEnum,
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
    let mut max = 0;
    for stmt in &result.stmts {
        if let Some(ref stmt) = stmt.stmt {
            find_max_param(&stmt.node, &mut max);
        }
    }
    max
}

fn find_max_param(node: &Option<NodeEnum>, max: &mut i32) {
    let Some(node) = node else {
        return;
    };

    match node {
        NodeEnum::ParamRef(param) => {
            if param.number > *max {
                *max = param.number;
            }
        }
        NodeEnum::TypeCast(cast) => {
            if let Some(ref arg) = cast.arg {
                find_max_param(&arg.node, max);
            }
        }
        NodeEnum::FuncCall(func) => {
            for arg in &func.args {
                find_max_param(&arg.node, max);
            }
        }
        NodeEnum::AExpr(expr) => {
            if let Some(ref lexpr) = expr.lexpr {
                find_max_param(&lexpr.node, max);
            }
            if let Some(ref rexpr) = expr.rexpr {
                find_max_param(&rexpr.node, max);
            }
        }
        NodeEnum::SelectStmt(stmt) => {
            for item in &stmt.target_list {
                find_max_param(&item.node, max);
            }
            for item in &stmt.values_lists {
                find_max_param(&item.node, max);
            }
            for item in &stmt.from_clause {
                find_max_param(&item.node, max);
            }
            if let Some(ref clause) = stmt.where_clause {
                find_max_param(&clause.node, max);
            }
            if let Some(ref limit) = stmt.limit_count {
                find_max_param(&limit.node, max);
            }
            if let Some(ref offset) = stmt.limit_offset {
                find_max_param(&offset.node, max);
            }
        }
        NodeEnum::InsertStmt(stmt) => {
            if let Some(ref select) = stmt.select_stmt {
                find_max_param(&select.node, max);
            }
        }
        NodeEnum::UpdateStmt(stmt) => {
            for item in &stmt.target_list {
                find_max_param(&item.node, max);
            }
            if let Some(ref clause) = stmt.where_clause {
                find_max_param(&clause.node, max);
            }
        }
        NodeEnum::DeleteStmt(stmt) => {
            if let Some(ref clause) = stmt.where_clause {
                find_max_param(&clause.node, max);
            }
        }
        NodeEnum::ResTarget(res) => {
            if let Some(ref val) = res.val {
                find_max_param(&val.node, max);
            }
        }
        NodeEnum::List(list) => {
            for item in &list.items {
                find_max_param(&item.node, max);
            }
        }
        NodeEnum::CoalesceExpr(coalesce) => {
            for arg in &coalesce.args {
                find_max_param(&arg.node, max);
            }
        }
        NodeEnum::CaseExpr(case) => {
            if let Some(ref arg) = case.arg {
                find_max_param(&arg.node, max);
            }
            for when in &case.args {
                find_max_param(&when.node, max);
            }
            if let Some(ref defresult) = case.defresult {
                find_max_param(&defresult.node, max);
            }
        }
        NodeEnum::CaseWhen(when) => {
            if let Some(ref expr) = when.expr {
                find_max_param(&expr.node, max);
            }
            if let Some(ref result) = when.result {
                find_max_param(&result.node, max);
            }
        }
        NodeEnum::BoolExpr(expr) => {
            for arg in &expr.args {
                find_max_param(&arg.node, max);
            }
        }
        NodeEnum::NullTest(test) => {
            if let Some(ref arg) = test.arg {
                find_max_param(&arg.node, max);
            }
        }
        NodeEnum::ExplainStmt(stmt) => {
            if let Some(ref query) = stmt.query {
                find_max_param(&query.node, max);
            }
        }
        _ => {}
    }
}
