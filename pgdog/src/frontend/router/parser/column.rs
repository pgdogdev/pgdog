//! Column name reference.

use pg_query::{
    protobuf::{self, String as PgQueryString},
    Node, NodeEnum,
};
use std::fmt::{Display, Formatter, Result as FmtResult};

use super::Table;
use crate::util::escape_identifier;

/// Column name extracted from a query.
#[derive(Debug, Clone, Copy, PartialEq, Default)]
pub struct Column<'a> {
    /// Column name.
    pub name: &'a str,
    /// Table name.
    pub table: Option<&'a str>,
    /// Schema name.
    pub schema: Option<&'a str>,
}

impl<'a> Column<'a> {
    pub fn table(&self) -> Option<Table<'a>> {
        if let Some(table) = self.table {
            Some(Table {
                name: table,
                schema: self.schema.clone(),
            })
        } else {
            None
        }
    }
}

impl<'a> Column<'a> {
    pub fn from_string(string: &'a Node) -> Result<Self, ()> {
        match &string.node {
            Some(NodeEnum::String(protobuf::String { sval })) => Ok(Self {
                name: sval.as_str(),
                ..Default::default()
            }),

            _ => Err(()),
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

impl<'a> TryFrom<&'a Node> for Column<'a> {
    type Error = ();

    fn try_from(value: &'a Node) -> Result<Self, Self::Error> {
        Column::try_from(&value.node)
    }
}

impl<'a> TryFrom<&'a Option<NodeEnum>> for Column<'a> {
    type Error = ();

    fn try_from(value: &'a Option<NodeEnum>) -> Result<Self, Self::Error> {
        fn from_node(node: &Node) -> Option<&str> {
            if let Some(NodeEnum::String(PgQueryString { sval })) = &node.node {
                Some(sval.as_str())
            } else {
                None
            }
        }

        fn from_slice<'a>(nodes: &'a [Node]) -> Result<Column<'a>, ()> {
            match nodes.len() {
                3 => {
                    let schema = nodes.iter().nth(0).map(from_node).flatten();
                    let table = nodes.iter().nth(1).map(from_node).flatten();
                    let name = nodes.iter().nth(2).map(from_node).flatten().ok_or(())?;

                    return Ok(Column {
                        schema,
                        table,
                        name,
                    });
                }

                2 => {
                    let table = nodes.iter().nth(0).map(from_node).flatten();
                    let name = nodes.iter().nth(1).map(from_node).flatten().ok_or(())?;

                    return Ok(Column {
                        schema: None,
                        table,
                        name,
                    });
                }

                1 => {
                    let name = nodes.iter().nth(0).map(from_node).flatten().ok_or(())?;

                    return Ok(Column {
                        name,
                        ..Default::default()
                    });
                }

                _ => return Err(()),
            }
        }

        match value {
            Some(NodeEnum::ResTarget(res_target)) => {
                return Ok(Self {
                    name: res_target.name.as_str(),
                    ..Default::default()
                });
            }

            Some(NodeEnum::List(list)) => from_slice(&list.items),

            Some(NodeEnum::ColumnRef(column_ref)) => from_slice(&column_ref.fields),

            Some(NodeEnum::DefElem(list)) => {
                if list.defname == "owned_by" {
                    if let Some(ref node) = list.arg {
                        Ok(Column::try_from(&node.node)?)
                    } else {
                        Err(())
                    }
                } else {
                    Err(())
                }
            }

            _ => return Err(()),
        }
    }
}

impl<'a> TryFrom<&Option<&'a Node>> for Column<'a> {
    type Error = ();

    fn try_from(value: &Option<&'a Node>) -> Result<Self, Self::Error> {
        if let Some(value) = value {
            (*value).try_into()
        } else {
            Err(())
        }
    }
}

#[cfg(test)]
mod test {
    use pg_query::{parse, NodeEnum};

    use super::Column;

    #[test]
    fn test_column() {
        let query = parse("INSERT INTO my_table (id, email) VALUES (1, 'test@test.com')").unwrap();
        let select = query.protobuf.stmts.first().unwrap().stmt.as_ref().unwrap();
        match select.node {
            Some(NodeEnum::InsertStmt(ref insert)) => {
                let columns = insert
                    .cols
                    .iter()
                    .map(Column::try_from)
                    .collect::<Result<Vec<Column>, ()>>()
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
