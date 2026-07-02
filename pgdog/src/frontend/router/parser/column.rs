//! Column name reference.

use pg_query::{
    Node, NodeEnum,
    protobuf::{self, String as PgQueryString},
};
#[cfg(feature = "new_parser")]
use pg_raw_parse::{list, nodes};
use std::fmt::{Display, Formatter, Result as FmtResult};

use super::{Error, Table};
use crate::util::escape_identifier;

/// Column name extracted from a query.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Default)]
pub struct Column<'a> {
    /// Column name.
    pub name: &'a str,
    /// Table name.
    pub table: Option<&'a str>,
    /// Schema name.
    pub schema: Option<&'a str>,
}

/// Owned version of Column that owns its string data.
#[derive(Debug, Clone, PartialEq, Default)]
pub struct OwnedColumn {
    /// Column name.
    pub name: String,
    /// Table name.
    pub table: Option<String>,
    /// Schema name.
    pub schema: Option<String>,
}

impl<'a> Column<'a> {
    pub fn table(&self) -> Option<Table<'a>> {
        self.table.map(|table| Table {
            name: table,
            schema: self.schema,
            alias: None,
        })
    }

    /// Convert this borrowed Column to an owned OwnedColumn
    pub fn to_owned(&self) -> OwnedColumn {
        OwnedColumn::from(*self)
    }

    pub fn from_string(string: &'a Node) -> Result<Self, ()> {
        match &string.node {
            Some(NodeEnum::String(protobuf::String { sval })) => Ok(Self {
                name: sval.as_str(),
                ..Default::default()
            }),

            _ => Err(()),
        }
    }

    /// Fully-qualify this column with a table.
    pub fn qualify(&mut self, table: Table<'a>) {
        if self.table.is_none() {
            self.table = Some(table.name);
            self.schema = table.schema;
        }
    }
}

impl<'a> Display for Column<'a> {
    fn fmt(&self, f: &mut Formatter<'_>) -> FmtResult {
        match (self.schema, self.table) {
            (Some(schema), Some(table)) => {
                write!(
                    f,
                    "\"{}\".\"{}\".\"{}\"",
                    escape_identifier(schema),
                    escape_identifier(table),
                    escape_identifier(self.name)
                )
            }
            (None, Some(table)) => {
                write!(
                    f,
                    "\"{}\".\"{}\"",
                    escape_identifier(table),
                    escape_identifier(self.name)
                )
            }
            _ => {
                write!(f, "\"{}\"", escape_identifier(self.name))
            }
        }
    }
}

impl Display for OwnedColumn {
    fn fmt(&self, f: &mut Formatter<'_>) -> FmtResult {
        let borrowed = Column::from(self);
        borrowed.fmt(f)
    }
}

impl<'a> From<Column<'a>> for OwnedColumn {
    fn from(column: Column<'a>) -> Self {
        Self {
            name: column.name.to_owned(),
            table: column.table.map(|s| s.to_owned()),
            schema: column.schema.map(|s| s.to_owned()),
        }
    }
}

impl<'a> From<&'a OwnedColumn> for Column<'a> {
    fn from(owned: &'a OwnedColumn) -> Self {
        Self {
            name: &owned.name,
            table: owned.table.as_deref(),
            schema: owned.schema.as_deref(),
        }
    }
}

#[cfg(feature = "new_parser")]
impl<'a> TryFrom<pg_raw_parse::Node<'a>> for Column<'a> {
    type Error = Error;

    fn try_from(value: pg_raw_parse::Node<'a>) -> Result<Self, Self::Error> {
        use pg_raw_parse::Node;
        match value {
            Node::ColumnRef(c) => Self::try_from(c),
            Node::ResTarget(r) => Self::try_from(r),
            Node::DefElem(d) if d.defname() == Some("owned_by") => Self::try_from(d.arg()),
            Node::NodeList(l) => Self::try_from(l),
            _ => Err(Error::ColumnDecode),
        }
    }
}

#[cfg(feature = "new_parser")]
impl<'a> TryFrom<&'a nodes::ColumnRef> for Column<'a> {
    type Error = Error;

    fn try_from(value: &'a nodes::ColumnRef) -> Result<Self, Self::Error> {
        Self::try_from(value.fields())
    }
}

#[cfg(feature = "new_parser")]
impl<'a> TryFrom<&'a nodes::ResTarget> for Column<'a> {
    type Error = Error;

    fn try_from(value: &'a nodes::ResTarget) -> Result<Self, Self::Error> {
        Ok(Self {
            name: value.name().ok_or(Error::ColumnDecode)?,
            ..Default::default()
        })
    }
}

#[cfg(feature = "new_parser")]
impl<'a> TryFrom<&'a list::NodeList> for Column<'a> {
    type Error = Error;

    fn try_from(value: &'a list::NodeList) -> Result<Self, Self::Error> {
        let mut fields = value
            .into_iter()
            .map(|s| s.as_str().ok_or(Error::ColumnDecode));
        let name = fields.next_back().ok_or(Error::ColumnDecode)??;
        let table = fields.next_back().transpose()?;
        let schema = fields.next_back().transpose()?;

        if fields.len() == 0 {
            Ok(Self {
                name,
                table,
                schema,
            })
        } else {
            Err(Error::ColumnDecode)
        }
    }
}

impl<'a> TryFrom<&'a Node> for Column<'a> {
    type Error = Error;

    fn try_from(value: &'a Node) -> Result<Self, Self::Error> {
        Column::try_from(&value.node)
    }
}

impl<'a> TryFrom<&'a Option<NodeEnum>> for Column<'a> {
    type Error = Error;

    fn try_from(value: &'a Option<NodeEnum>) -> Result<Self, Self::Error> {
        fn from_node(node: &Node) -> Option<&str> {
            if let Some(NodeEnum::String(PgQueryString { sval })) = &node.node {
                Some(sval.as_str())
            } else {
                None
            }
        }

        fn from_slice<'a>(nodes: &'a [Node]) -> Result<Column<'a>, Error> {
            match nodes.len() {
                3 => {
                    let schema = nodes.first().and_then(from_node);
                    let table = nodes.get(1).and_then(from_node);
                    let name = nodes
                        .get(2)
                        .and_then(from_node)
                        .ok_or(Error::ColumnDecode)?;

                    Ok(Column {
                        schema,
                        table,
                        name,
                    })
                }

                2 => {
                    let table = nodes.first().and_then(from_node);
                    let name = nodes
                        .get(1)
                        .and_then(from_node)
                        .ok_or(Error::ColumnDecode)?;

                    Ok(Column {
                        schema: None,
                        table,
                        name,
                    })
                }

                1 => {
                    let name = nodes
                        .first()
                        .and_then(from_node)
                        .ok_or(Error::ColumnDecode)?;

                    Ok(Column {
                        name,
                        ..Default::default()
                    })
                }

                _ => Err(Error::ColumnDecode),
            }
        }

        match value {
            Some(NodeEnum::ResTarget(res_target)) => Ok(Self {
                name: res_target.name.as_str(),
                ..Default::default()
            }),

            Some(NodeEnum::List(list)) => from_slice(&list.items),

            Some(NodeEnum::ColumnRef(column_ref)) => from_slice(&column_ref.fields),

            Some(NodeEnum::DefElem(list)) => {
                if list.defname == "owned_by" {
                    if let Some(ref node) = list.arg {
                        Ok(Column::try_from(&node.node)?)
                    } else {
                        Err(Error::ColumnDecode)
                    }
                } else {
                    Err(Error::ColumnDecode)
                }
            }

            _ => Err(Error::ColumnDecode),
        }
    }
}

impl<'a> TryFrom<&Option<&'a Node>> for Column<'a> {
    type Error = Error;

    fn try_from(value: &Option<&'a Node>) -> Result<Self, Self::Error> {
        if let Some(value) = value {
            (*value).try_into()
        } else {
            Err(Error::ColumnDecode)
        }
    }
}

impl<'a> From<&'a str> for Column<'a> {
    fn from(value: &'a str) -> Self {
        Column {
            name: value,
            table: None,
            schema: None,
        }
    }
}

#[cfg(test)]
mod test {
    #[cfg(feature = "new_parser")]
    use itertools::*;
    #[cfg(not(feature = "new_parser"))]
    use pg_query::{NodeEnum, parse};
    #[cfg(feature = "new_parser")]
    use pg_raw_parse::*;

    use super::Column;

    #[test]
    #[cfg(feature = "new_parser")]
    fn test_column() {
        let result = parse("INSERT INTO my_table (id, email) VALUES (1, 'test@test.com')").unwrap();
        let stmt = result.stmts().exactly_one().ok().unwrap();

        let Node::InsertStmt(i) = stmt else {
            panic!("{:?} is not an insert stmt", stmt);
        };
        let columns = i
            .cols()
            .into_iter()
            .map(Column::try_from)
            .collect::<Result<Vec<_>, _>>()
            .unwrap();
        std::assert_matches!(
            &*columns,
            &[Column { name: "id", .. }, Column { name: "email", .. }]
        );
    }

    #[test]
    #[cfg(not(feature = "new_parser"))]
    fn test_column() {
        let query = parse("INSERT INTO my_table (id, email) VALUES (1, 'test@test.com')").unwrap();
        let select = query.protobuf.stmts.first().unwrap().stmt.as_ref().unwrap();
        match select.node {
            Some(NodeEnum::InsertStmt(ref insert)) => {
                let columns = insert
                    .cols
                    .iter()
                    .map(Column::try_from)
                    .collect::<Result<Vec<_>, _>>()
                    .unwrap();
                assert_eq!(
                    columns,
                    vec![
                        Column {
                            name: "id",
                            ..Default::default()
                        },
                        Column {
                            name: "email",
                            ..Default::default()
                        }
                    ]
                );
            }

            _ => panic!("not a select"),
        }
    }

    #[test]
    #[cfg(feature = "new_parser")]
    fn test_column_sequence() {
        let result =
            parse("ALTER SEQUENCE public.user_profiles_id_seq OWNED BY public.user_profiles.id")
                .unwrap();
        let stmt = result.stmts().exactly_one().ok().unwrap();

        let Node::AlterSeqStmt(stmt) = stmt else {
            panic!("not an alter sequence");
        };

        let column = Column::try_from(stmt.options().into_iter().exactly_one().unwrap()).unwrap();
        assert_eq!(
            column,
            Column {
                name: "id",
                schema: Some("public"),
                table: Some("user_profiles")
            }
        );
    }

    #[test]
    #[cfg(not(feature = "new_parser"))]
    fn test_column_sequence() {
        let query =
            parse("ALTER SEQUENCE public.user_profiles_id_seq OWNED BY public.user_profiles.id")
                .unwrap();
        let alter = query.protobuf.stmts.first().unwrap().stmt.as_ref().unwrap();
        match alter.node {
            Some(NodeEnum::AlterSeqStmt(ref stmt)) => {
                if let Some(node) = stmt.options.first() {
                    let column = Column::try_from(node).unwrap();
                    assert_eq!(column.name, "id");
                    assert_eq!(column.schema, Some("public"));
                    assert_eq!(column.table, Some("user_profiles"));
                } else {
                    panic!("no owned by clause");
                }
            }
            _ => panic!("not an alter sequence"),
        }
    }
}
