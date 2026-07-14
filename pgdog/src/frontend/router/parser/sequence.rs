use std::fmt::Display;

use super::{Column, Table, error::Error};
use crate::util::escape_identifier;

/// Sequence name in a query.
#[derive(Debug, Clone, Copy, PartialEq, Default)]
pub(crate) struct Sequence<'a> {
    /// Table representing the sequence name and schema.
    pub(crate) table: Table<'a>,
}

impl Display for Sequence<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.table.fmt(f)
    }
}

impl<'a> Sequence<'a> {
    /// Generate a setval statement to set the sequence to the max value of the given column
    pub(crate) fn setval_from_column(&self, column: &Column<'a>) -> Result<String, Error> {
        let sequence_name = self.table.to_string();

        let table = column.table().ok_or(Error::ColumnNoTable)?;
        let table_name = table.to_string();

        let column_name = format!("\"{}\"", escape_identifier(column.name));

        Ok(format!(
            "SELECT setval('{}', COALESCE((SELECT MAX({}) FROM {}), 1), true);",
            sequence_name, column_name, table_name
        ))
    }
}

impl<'a> From<Table<'a>> for Sequence<'a> {
    fn from(table: Table<'a>) -> Self {
        Self { table }
    }
}

#[cfg(test)]
mod test {
    use super::{Column, Sequence, Table};

    #[test]
    fn test_sequence_setval_from_alter_statement() {
        let sequence = Sequence::from(Table {
            schema: Some("public"),
            name: "user_profiles_id_seq",
            alias: None,
        });

        let setval_sql = sequence
            .setval_from_column(&Column {
                schema: Some("public"),
                table: Some("user_profiles"),
                name: "id",
            })
            .unwrap();
        assert_eq!(
            setval_sql,
            "SELECT setval('\"public\".\"user_profiles_id_seq\"', COALESCE((SELECT MAX(\"id\") FROM \"public\".\"user_profiles\"), 1), true);"
        );
    }

    #[test]
    fn test_sequence_display() {
        let table = Table {
            name: "my_seq",
            schema: Some("public"),
            alias: None,
        };
        let sequence = Sequence::from(table);

        assert_eq!(sequence.to_string(), "\"public\".\"my_seq\"");
    }

    #[test]
    fn test_sequence_display_no_schema() {
        let table = Table {
            name: "my_seq",
            schema: None,
            alias: None,
        };
        let sequence = Sequence::from(table);

        assert_eq!(sequence.to_string(), "\"my_seq\"");
    }
}
