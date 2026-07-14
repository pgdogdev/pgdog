//! WHERE clause of a UPDATE/SELECT/DELETE query.

#[cfg(not(feature = "new_parser"))]
use pg_query::{
    NodeEnum,
    protobuf::{a_const::Val, *},
};
#[cfg(feature = "new_parser")]
use pg_raw_parse::{ConstValue, Node, nodes};
use std::string::String;

use crate::frontend::router::parser::{FromClause, Table};

use super::Key;

#[derive(Copy, Clone, Debug)]
pub(crate) enum TablesSource<'a> {
    Table(Table<'a>),
    FromClause(FromClause<'a>),
}

impl<'a> From<Table<'a>> for TablesSource<'a> {
    fn from(value: Table<'a>) -> Self {
        Self::Table(value)
    }
}

impl<'a> From<FromClause<'a>> for TablesSource<'a> {
    fn from(value: FromClause<'a>) -> Self {
        Self::FromClause(value)
    }
}

impl<'a> TablesSource<'a> {
    pub(crate) fn resolve_alias(&self, name: &'a str) -> &'a str {
        match self {
            Self::Table(table) => {
                if table.name_match(name) {
                    table.name
                } else {
                    name
                }
            }
            Self::FromClause(fc) => {
                if let Some(name) = fc.resolve_alias(name) {
                    name
                } else {
                    name
                }
            }
        }
    }

    pub(crate) fn table_name(&self) -> Option<&'a str> {
        match self {
            Self::Table(table) => Some(table.name),
            Self::FromClause(fc) => fc.table_name(),
        }
    }
}

#[derive(Debug)]
pub(crate) struct Column<'a> {
    /// Table name if fully qualified.
    /// Can be an alias.
    pub(crate) table: Option<&'a str>,
    /// Column name.
    pub(crate) name: &'a str,
}

#[derive(Debug)]
enum Output<'a> {
    Parameter { pos: i32, array: bool },
    Value { value: String, array: bool },
    Int { value: i32, array: bool },
    Column(Column<'a>),
    NullCheck(Column<'a>),
    Filter(Vec<Output<'a>>, Vec<Output<'a>>),
}

/// Parse `WHERE` clause of a statement looking for sharding keys.
#[derive(Debug)]
pub(crate) struct WhereClause<'a> {
    output: Vec<Output<'a>>,
}

impl<'a> WhereClause<'a> {
    /// Parse the `WHERE` clause of a statement and extract
    /// all possible sharding keys.
    #[cfg(feature = "new_parser")]
    pub(crate) fn new(source: &TablesSource<'a>, where_clause: Node<'a>) -> Option<Self> {
        if let Node::None = where_clause {
            return None;
        };

        let output = Self::parse(source, where_clause, false);

        Some(Self { output })
    }

    cfg_select! {
        not(feature = "new_parser") => {
            pub(crate) fn new(
                source: &TablesSource<'a>,
                where_clause: &'a Option<Box<Node>>,
            ) -> Option<WhereClause<'a>> {
                let Some(where_clause) = where_clause else {
                    return None;
                };

                let output = Self::parse(source, where_clause, false);

                Some(Self { output })
            }
        }
        _ => {}
    }

    pub(crate) fn keys(&self, table_name: Option<&str>, column_name: &str) -> Vec<Key> {
        let mut keys = vec![];
        for output in &self.output {
            keys.extend(Self::search_for_keys(output, table_name, column_name));
        }
        keys
    }

    fn column_match(column: &Column, table: Option<&str>, name: &str) -> bool {
        if let (Some(table), Some(other_table)) = (table, &column.table)
            && &table != other_table
        {
            return false;
        };

        column.name == name
    }

    fn get_key(output: &Output) -> Option<Key> {
        match output {
            Output::Int { value, array } => Some(Key::Constant {
                value: value.to_string(),
                array: *array,
            }),
            Output::Parameter { pos, array } => Some(Key::Parameter {
                pos: *pos as usize - 1,
                array: *array,
            }),
            Output::Value { value, array } => Some(Key::Constant {
                value: value.to_string(),
                array: *array,
            }),
            _ => None,
        }
    }

    fn search_for_keys(output: &Output, table_name: Option<&str>, column_name: &str) -> Vec<Key> {
        let mut keys = vec![];

        if let Output::Filter(left, right) = output {
            let left = left.as_slice();
            let right = right.as_slice();

            match (&left, &right) {
                // TODO: Handle something like
                // id = (SELECT 5) which is stupid but legal SQL.
                (&[Output::Column(column)], output) => {
                    if Self::column_match(column, table_name, column_name) {
                        for output in output.iter() {
                            if let Some(key) = Self::get_key(output) {
                                keys.push(key);
                            }
                        }
                    }
                }
                (output, &[Output::Column(column)]) => {
                    if Self::column_match(column, table_name, column_name) {
                        for output in output.iter() {
                            if let Some(key) = Self::get_key(output) {
                                keys.push(key);
                            }
                        }
                    }
                }

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

        if let Output::NullCheck(c) = output
            && c.name == column_name
            && c.table == table_name
        {
            keys.push(Key::Null);
        }

        keys
    }

    #[cfg(not(feature = "new_parser"))]
    fn string(node: Option<&Node>) -> Option<&str> {
        if let Some(node) = node
            && let Some(NodeEnum::String(ref string)) = node.node
        {
            return Some(string.sval.as_str());
        }

        None
    }

    #[cfg(feature = "new_parser")]
    fn parse(source: &TablesSource<'a>, node: Node<'a>, array: bool) -> Vec<Output<'a>> {
        match node {
            // Only check for IS NULL, IS NOT NULL definitely doesn't help.
            Node::NullTest(null_test) if null_test.nulltesttype == nodes::NullTestType::IS_NULL => {
                Self::parse(source, null_test.arg(), array)
                    .into_iter()
                    .filter_map(|arg| match arg {
                        Output::Column(c) => Some(Output::NullCheck(c)),
                        _ => None,
                    })
                    .collect()
            }

            // Only AND expressions can really be asserted.
            // OR needs both sides to be evaluated and either one
            // can direct to a shard. Most cases, this will end up on all shards.
            Node::BoolExpr(expr) if expr.boolop == nodes::BoolExprType::AND_EXPR => expr
                .args()
                .iter()
                .flat_map(|arg| Self::parse(source, arg, array))
                .collect(),

            Node::A_Expr(expr) => {
                use nodes::A_Expr_Kind;
                if matches!(
                    expr.kind,
                    A_Expr_Kind::AEXPR_OP | A_Expr_Kind::AEXPR_IN | A_Expr_Kind::AEXPR_OP_ANY
                ) {
                    if expr.name().first().and_then(Node::as_str) != Some("=") {
                        return Vec::new();
                    }
                }
                let array = matches!(expr.kind, A_Expr_Kind::AEXPR_OP_ANY);
                let left = Self::parse(source, expr.lexpr(), array);
                let right = Self::parse(source, expr.rexpr(), array);

                vec![Output::Filter(left, right)]
            }

            Node::A_Const(value) => value
                .val()
                .into_iter()
                .filter_map(|val| match val {
                    ConstValue::Integer(value) => Some(Output::Int { value, array }),
                    ConstValue::String(s) | ConstValue::Float(s) => Some(Output::Value {
                        value: s.to_owned(),
                        array,
                    }),
                    _ => None,
                })
                .collect(),

            Node::ColumnRef(column) => {
                let mut fields = column.fields().into_iter().rev();
                let name = fields.next().and_then(Node::as_str);
                let table = fields.next().and_then(Node::as_str);
                let table = if let Some(table) = table {
                    Some(source.resolve_alias(table))
                } else {
                    source.table_name()
                };

                name.into_iter()
                    .map(|name| Output::Column(Column { name, table }))
                    .collect()
            }

            Node::ParamRef(param) => {
                vec![Output::Parameter {
                    pos: param.number,
                    array,
                }]
            }

            Node::NodeList(list) => list
                .iter()
                .flat_map(|node| Self::parse(source, node, array))
                .collect(),

            Node::TypeCast(cast) => Self::parse(source, cast.arg(), array),

            _ => Vec::new(),
        }
    }

    cfg_select! {
        not(feature = "new_parser") => {
            fn parse(source: &TablesSource<'a>, node: &'a Node, array: bool) -> Vec<Output<'a>> {
                let mut keys = vec![];

                match node.node {
                    Some(NodeEnum::NullTest(ref null_test))
                        // Only check for IS NULL, IS NOT NULL definitely doesn't help.
                        if NullTestType::try_from(null_test.nulltesttype) == Ok(NullTestType::IsNull) => {
                            let left = null_test
                                .arg
                                .as_ref()
                                .and_then(|node| Self::parse(source, node, array).pop());

                            if let Some(Output::Column(c)) = left {
                                keys.push(Output::NullCheck(c));
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
                            keys.extend(Self::parse(source, arg, array));
                        }
                    }

                    Some(NodeEnum::AExpr(ref expr)) => {
                        let kind = expr.kind();
                        if matches!(
                            kind,
                            AExprKind::AexprOp | AExprKind::AexprIn | AExprKind::AexprOpAny
                        ) {
                            let op = Self::string(expr.name.first());
                            if let Some(op) = op
                                && op != "=" {
                                    return keys;
                                }
                        }
                        let array = matches!(kind, AExprKind::AexprOpAny);
                        if let Some(ref left) = expr.lexpr
                            && let Some(ref right) = expr.rexpr {
                                let left = Self::parse(source, left, array);
                                let right = Self::parse(source, right, array);

                                keys.push(Output::Filter(left, right));
                            }
                    }

                    Some(NodeEnum::AConst(ref value)) => {
                        if let Some(ref val) = value.val {
                            match val {
                                Val::Ival(int) => keys.push(Output::Int {
                                    value: int.ival,
                                    array,
                                }),
                                Val::Sval(sval) => keys.push(Output::Value {
                                    value: sval.sval.clone(),
                                    array,
                                }),
                                Val::Fval(fval) => keys.push(Output::Value {
                                    value: fval.fval.clone(),
                                    array,
                                }),
                                _ => (),
                            }
                        }
                    }

                    Some(NodeEnum::ColumnRef(ref column)) => {
                        let name = Self::string(column.fields.last());
                        let table = Self::string(column.fields.iter().rev().nth(1));
                        let table = if let Some(table) = table {
                            Some(source.resolve_alias(table))
                        } else {
                            source.table_name()
                        };

                        if let Some(name) = name {
                            return vec![Output::Column(Column { name, table })];
                        }
                    }

                    Some(NodeEnum::ParamRef(ref param)) => {
                        keys.push(Output::Parameter {
                            pos: param.number,
                            array,
                        });
                    }

                    Some(NodeEnum::List(ref list)) => {
                        for node in &list.items {
                            keys.extend(Self::parse(source, node, array));
                        }
                    }

                    Some(NodeEnum::TypeCast(ref cast)) => {
                        if let Some(ref arg) = cast.arg {
                            keys.extend(Self::parse(source, arg, array));
                        }
                    }

                    _ => (),
                };

                keys
            }
        }
        _ => {}
    }
}

#[cfg(test)]
mod test {
    #[cfg(not(feature = "new_parser"))]
    use pg_query::{ParseResult, parse};
    #[cfg(feature = "new_parser")]
    use pg_raw_parse::{ParseResult, parse};

    use super::*;

    #[test]
    fn test_where_clause() {
        let query =
            "SELECT * FROM sharded WHERE id = 5 AND (something_else != 6 OR column_a = 'test')";
        let ast = parse(query).unwrap();
        let where_ = where_clause(&ast);
        let mut keys = where_.keys(Some("sharded"), "id");
        assert_eq!(
            keys.pop().unwrap(),
            Key::Constant {
                value: "5".into(),
                array: false
            }
        );
    }

    #[test]
    fn test_is_null() {
        let query = "SELECT * FROM users WHERE tenant_id IS NULL";
        let ast = parse(query).unwrap();

        let where_ = where_clause(&ast);
        assert_eq!(
            where_.keys(Some("users"), "tenant_id").pop(),
            Some(Key::Null)
        );

        //  NOT NULL check is basically everyone, so no!
        let query = "SELECT * FROM users WHERE tenant_id IS NOT NULL";
        let ast = parse(query).unwrap();

        let where_ = where_clause(&ast);
        assert!(where_.keys(Some("users"), "tenant_id").is_empty());
    }

    #[test]
    fn test_in_clause() {
        let query = "SELECT * FROM users WHERE tenant_id IN ($1, $2, $3, $4)";
        let ast = parse(query).unwrap();
        let where_ = where_clause(&ast);

        let keys = where_.keys(Some("users"), "tenant_id");
        assert_eq!(keys.len(), 4);
    }

    #[test]
    fn test_any() {
        let query = "SELECT * FROM users WHERE tenant_id = ANY($1)";
        let ast = parse(query).unwrap();
        let where_ = where_clause(&ast);
        let keys = where_.keys(Some("users"), "tenant_id");
        assert_eq!(
            keys[0],
            Key::Parameter {
                pos: 0,
                array: true
            }
        );

        let query = "SELECT * FROM users WHERE tenant_id = ANY('{1, 2, 3}')";
        let ast = parse(query).unwrap();
        let where_ = where_clause(&ast);
        let keys = where_.keys(Some("users"), "tenant_id");
        assert_eq!(
            keys[0],
            Key::Constant {
                value: "{1, 2, 3}".to_string(),
                array: true
            },
        );
    }

    #[test]
    fn test_joins_with_multiple_tables() {
        let query = "SELECT * FROM users u
                     JOIN orders o ON u.id = o.user_id
                     JOIN products p ON o.product_id = p.id
                     JOIN categories c ON p.category_id = c.id
                     WHERE u.tenant_id = $1 AND o.status = 'shipped' AND p.price > 100";
        let ast = parse(query).unwrap();
        let where_ = where_clause(&ast);

        // Test that we can extract keys for the users table with alias 'u'
        let keys = where_.keys(Some("users"), "tenant_id");
        assert_eq!(keys.len(), 1);
        assert_eq!(
            keys[0],
            Key::Parameter {
                pos: 0,
                array: false
            }
        );

        // Test that we can extract keys for the orders table with alias 'o'
        let status_keys = where_.keys(Some("orders"), "status");
        assert_eq!(status_keys.len(), 1);
        assert_eq!(
            status_keys[0],
            Key::Constant {
                value: "shipped".to_string(),
                array: false
            }
        );
    }

    #[test]
    fn test_as_alias() {
        let query = r#"SELECT "id", "email", "createdAt", "updatedAt" FROM "Users" AS "User" WHERE "User"."id" = 24 ORDER BY "User"."id" LIMIT 1;"#;
        let ast = parse(query).unwrap();
        let where_ = where_clause(&ast);
        let keys = where_.keys(Some("Users"), "id");
        assert_eq!(
            keys[0],
            Key::Constant {
                value: "24".to_string(),
                array: false
            },
        );
    }

    #[cfg(feature = "new_parser")]
    fn where_clause(ast: &ParseResult) -> WhereClause<'_> {
        let Some(Node::SelectStmt(stmt)) = ast.stmts().next() else {
            panic!("Not a select");
        };
        let from_clause = FromClause::new(stmt.from_clause());
        let source = TablesSource::from(from_clause);
        WhereClause::new(&source, stmt.where_clause()).unwrap()
    }

    cfg_select! {
        not(feature = "new_parser") => {
            fn where_clause(ast: &ParseResult) -> WhereClause<'_> {
                let stmt = ast.protobuf.stmts.first().as_ref().unwrap().stmt.as_ref().unwrap();
                let Some(NodeEnum::SelectStmt(stmt)) = &stmt.node else {
                    panic!("Not a select");
                };
                let from_clause = FromClause::new(&stmt.from_clause);
                let source = TablesSource::from(from_clause);
                WhereClause::new(&source, &stmt.where_clause).unwrap()
            }
        }
        _ => {}
    }
}
