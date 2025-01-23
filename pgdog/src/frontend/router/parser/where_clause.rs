//! WHERE clause of a UPDATE/SELECT/DELETE query.

use pg_query::{
    protobuf::{a_const::Val, *},
    Error, NodeEnum,
};
use std::string::String;

pub struct Column {
    table: Option<String>,
    name: String,
}

pub enum Output {
    Parameter(usize),
    Value(String),
    Int(i32),
    Column(Column),
    Filter(Column, Box<Output>),
}

pub struct WhereClause {}

impl WhereClause {
    pub fn new(where_clause: &Option<Node>) -> Result<Option<WhereClause>, Error> {
        let Some(ref where_clause) = where_clause else {
            return Ok(None);
        };

        let Some(ref node) = where_clause.node else {
            return Ok(None);
        };

        match node {
            NodeEnum::BoolExpr(ref expr) => {
                for arg in &expr.args {
                    let Some(ref node) = arg.node else {
                        continue;
                    };

                    match node {
                        NodeEnum::AExpr(ref aexpr) => {}

                        _ => continue,
                    }
                }
            }

            _ => (),
        };

        todo!()
    }

    fn parse(node: &Node) -> Result<Vec<Output>, Error> {
        let mut keys = vec![];

        match node.node {
            Some(NodeEnum::BoolExpr(ref expr)) => {
                for arg in &expr.args {
                    keys.extend(Self::parse(arg)?);
                }
            }

            Some(NodeEnum::AExpr(ref expr)) => {
                if let Some(ref left) = expr.lexpr {}

                if let Some(ref right) = expr.rexpr {
                    keys.extend(Self::parse(right)?);
                }
            }

            Some(NodeEnum::AConst(ref value)) => {
                if let Some(ref val) = value.val {
                    match val {
                        Val::Ival(int) => keys.push(Output::Int(int.ival)),
                        Val::Sval(sval) => keys.push(Output::Value(sval.sval.clone())),
                        _ => (),
                    }
                }
            }

            _ => (),
        };

        Ok(keys)
    }

    fn column(node: &Node) -> Result<Option<Column>, Error> {
        fn string(node: Option<&Node>) -> Option<String> {
            if let Some(ref node) = node {
                match node.node {
                    Some(NodeEnum::String(ref string)) => return Some(string.sval.clone()),
                    _ => (),
                }
            }

            None
        }

        match node.node {
            Some(NodeEnum::ColumnRef(ref column)) => {
                let name = string(column.fields.last());
                let table = string(column.fields.iter().rev().nth(1));

                if let Some(name) = name {
                    return Ok(Some(Column { name, table }));
                }
            }

            _ => (),
        }

        Ok(None)
    }
}
