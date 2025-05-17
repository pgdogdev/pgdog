//! WHERE clause of a UPDATE/SELECT/DELETE query.

use pg_query::{
    protobuf::{a_const::Val, *},
    NodeEnum,
};
use std::string::String;

use super::Key;

#[derive(Debug)]
pub struct Column<'a> {
    /// Table name if fully qualified.
    /// Can be an alias.
    pub table: Option<&'a str>,
    /// Column name.
    pub name: &'a str,
}

#[derive(Debug)]
enum Output<'a> {
    Parameter(i32),
    Value(String),
    Int(i32),
    Column(Column<'a>),
    NullCheck(Column<'a>),
    Filter(Vec<Output<'a>>, Vec<Output<'a>>),
}

/// Parse `WHERE` clause of a statement looking for sharding keys.
#[derive(Debug)]
pub struct WhereClause<'a> {
    output: Vec<Output<'a>>,
}

impl<'a> WhereClause<'a> {
    /// Parse the `WHERE` clause of a statement and extract
    /// all possible sharding keys.
    pub fn new(
        table_name: Option<&'a str>,
        where_clause: &'a Option<Box<Node>>,
    ) -> Option<WhereClause<'a>> {
        let Some(ref where_clause) = where_clause else {
            return None;
        };

        let output = Self::parse(table_name, where_clause);

        Some(Self { output })
    }

    pub fn keys(&self, table_name: Option<&str>, column_name: &str) -> Vec<Key> {
        let mut keys = vec![];
        for output in &self.output {
            keys.extend(Self::search_for_keys(output, table_name, column_name));
        }
        keys
    }

    fn column_match(column: &Column, table: Option<&str>, name: &str) -> bool {
        if let (Some(table), Some(other_table)) = (table, &column.table) {
            if &table != other_table {
                return false;
            }
        };

        column.name == name
    }

    fn get_key(output: &Output) -> Option<Key> {
        match output {
            Output::Int(value) => Some(Key::Constant(value.to_string())),
            Output::Parameter(param) => Some(Key::Parameter(*param as usize - 1)),
            Output::Value(val) => Some(Key::Constant(val.to_string())),
            _ => None,
        }
    }

    fn search_for_keys(output: &Output, table_name: Option<&str>, column_name: &str) -> Vec<Key> {
        let mut keys = vec![];

        if let Output::Filter(ref left, ref right) = output {
            let left = left.as_slice();
            let right = right.as_slice();

            match (&left, &right) {
                // TODO: Handle something like
                // id = (SELECT 5) which is stupid but legal SQL.
                (&[left], &[right]) => match (left, right) {
                    (Output::Column(ref column), output) => {
                        if Self::column_match(column, table_name, column_name) {
                            if let Some(key) = Self::get_key(output) {
                                keys.push(key);
                            }
                        }
                    }
                    (output, Output::Column(ref column)) => {
                        if Self::column_match(column, table_name, column_name) {
                            if let Some(key) = Self::get_key(output) {
                                keys.push(key);
                            }
                        }
                    }
                    _ => (),
                },

                _ => {
                    for output in left {
                        keys.extend(Self::search_for_keys(output, table_name, column_name));
                    }

                    for output in right {
                        keys.extend(Self::search_for_keys(output, table_name, column_name));
                    }
                }
            }
        }

        if let Output::NullCheck(c) = output {
            if c.name == column_name && c.table == table_name {
                keys.push(Key::Null);
            }
        }

        keys
    }

    fn string(node: Option<&Node>) -> Option<&str> {
        if let Some(node) = node {
            if let Some(NodeEnum::String(ref string)) = node.node {
                return Some(string.sval.as_str());
            }
        }

        None
    }

    fn parse(table_name: Option<&'a str>, node: &'a Node) -> Vec<Output<'a>> {
        let mut keys = vec![];

        match node.node {
            Some(NodeEnum::NullTest(ref null_test)) => {
                // Only check for IS NULL, IS NOT NULL definitely doesn't help.
                if NullTestType::try_from(null_test.nulltesttype) == Ok(NullTestType::IsNull) {
                    let left = null_test
                        .arg
                        .as_ref()
                        .and_then(|node| Self::parse(table_name, node).pop());

                    if let Some(Output::Column(c)) = left {
                        keys.push(Output::NullCheck(c));
                    }
                }
            }

            Some(NodeEnum::BoolExpr(ref expr)) => {
                // Only AND expressions can really be asserted.
                // OR needs both sides to be evaluated and either one
                // can direct to a shard. Most cases, this will end up on all shards.
                if expr.boolop() != BoolExprType::AndExpr {
                    return keys;
                }

                for arg in &expr.args {
                    keys.extend(Self::parse(table_name, arg));
                }
            }

            Some(NodeEnum::AExpr(ref expr)) => {
                if expr.kind() == AExprKind::AexprOp {
                    let op = Self::string(expr.name.first());
                    if let Some(op) = op {
                        if op != "=" {
                            return keys;
                        }
                    }
                }
                if let Some(ref left) = expr.lexpr {
                    if let Some(ref right) = expr.rexpr {
                        let left = Self::parse(table_name, left);
                        let right = Self::parse(table_name, right);

                        keys.push(Output::Filter(left, right));
                    }
                }
            }

            Some(NodeEnum::AConst(ref value)) => {
                if let Some(ref val) = value.val {
                    match val {
                        Val::Ival(int) => keys.push(Output::Int(int.ival)),
                        Val::Sval(sval) => keys.push(Output::Value(sval.sval.clone())),
                        Val::Fval(fval) => keys.push(Output::Value(fval.fval.clone())),
                        _ => (),
                    }
                }
            }

            Some(NodeEnum::ColumnRef(ref column)) => {
                let name = Self::string(column.fields.last());
                let table = Self::string(column.fields.iter().rev().nth(1));
                let table = if let Some(table) = table {
                    Some(table)
                } else {
                    table_name
                };

                if let Some(name) = name {
                    return vec![Output::Column(Column { name, table })];
                }
            }

            Some(NodeEnum::ParamRef(ref param)) => {
                keys.push(Output::Parameter(param.number));
            }

            Some(NodeEnum::TypeCast(ref cast)) => {
                if let Some(ref arg) = cast.arg {
                    keys.extend(Self::parse(table_name, arg));
                }
            }

            _ => (),
        };

        keys
    }
}

#[cfg(test)]
mod test {
    use pg_query::parse;

    use super::*;

    #[test]
    fn test_where_clause() {
        let query =
            "SELECT * FROM sharded WHERE id = 5 AND (something_else != 6 OR column_a = 'test')";
        let ast = parse(query).unwrap();
        let stmt = ast.protobuf.stmts.first().cloned().unwrap().stmt.unwrap();

        if let Some(NodeEnum::SelectStmt(stmt)) = stmt.node {
            let where_ = WhereClause::new(Some("sharded"), &stmt.where_clause).unwrap();
            let mut keys = where_.keys(Some("sharded"), "id");
            assert_eq!(keys.pop().unwrap(), Key::Constant("5".into()));
        }
    }

    #[test]
    fn test_is_null() {
        let query = "SELECT * FROM users WHERE tenant_id IS NULL";
        let ast = parse(query).unwrap();

        let stmt = ast.protobuf.stmts.first().cloned().unwrap().stmt.unwrap();

        if let Some(NodeEnum::SelectStmt(stmt)) = stmt.node {
            let where_ = WhereClause::new(Some("users"), &stmt.where_clause).unwrap();
            assert_eq!(
                where_.keys(Some("users"), "tenant_id").pop(),
                Some(Key::Null)
            );
        }

        //  NOT NULL check is basically everyone, so no!
        let query = "SELECT * FROM users WHERE tenant_id IS NOT NULL";
        let ast = parse(query).unwrap();

        let stmt = ast.protobuf.stmts.first().cloned().unwrap().stmt.unwrap();

        if let Some(NodeEnum::SelectStmt(stmt)) = stmt.node {
            let where_ = WhereClause::new(Some("users"), &stmt.where_clause).unwrap();
            assert!(where_.keys(Some("users"), "tenant_id").is_empty());
        }
    }
}
