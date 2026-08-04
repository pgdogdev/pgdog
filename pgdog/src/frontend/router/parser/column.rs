//! Column name reference.

use pg_raw_parse::{list, nodes};
use std::fmt::{Display, Formatter, Result as FmtResult};

use super::{Error, Table};
use crate::util::escape_identifier;

/// Column name extracted from a query.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Default)]
pub(crate) struct Column<'a> {
    /// Column name.
    pub(crate) name: &'a str,
    /// Table name.
    pub(crate) table: Option<&'a str>,
    /// Schema name.
    pub(crate) schema: Option<&'a str>,
}

impl<'a> Column<'a> {
    pub(crate) fn table(&self) -> Option<Table<'a>> {
        self.table.map(|table| Table {
            name: table,
            schema: self.schema,
            alias: None,
        })
    }

    /// Fully-qualify this column with a table.
    pub(crate) fn qualify(&mut self, table: Table<'a>) {
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

impl<'a> TryFrom<&'a nodes::ColumnRef> for Column<'a> {
    type Error = Error;

    fn try_from(value: &'a nodes::ColumnRef) -> Result<Self, Self::Error> {
        Self::try_from(value.fields())
    }
}

impl<'a> TryFrom<&'a nodes::ResTarget> for Column<'a> {
    type Error = Error;

    fn try_from(value: &'a nodes::ResTarget) -> Result<Self, Self::Error> {
        Ok(Self {
            name: value.name().ok_or(Error::ColumnDecode)?,
            ..Default::default()
        })
    }
}

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
    use itertools::*;
    use pg_raw_parse::*;

    use super::Column;

    #[test]
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
}
