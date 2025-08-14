use std::fmt::Display;

use super::{error::Error, Column, OwnedTable, Table};
use crate::util::escape_identifier;

/// Sequence name in a query.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct Sequence<'a> {
    /// Table representing the sequence name and schema.
    pub table: Table<'a>,
}

/// Owned version of Sequence that owns its string data.
#[derive(Debug, Clone, PartialEq)]
pub struct OwnedSequence {
    /// Table representing the sequence name and schema.
    pub table: OwnedTable,
}

impl Display for Sequence<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.table.fmt(f)
    }
}

impl Default for Sequence<'_> {
    fn default() -> Self {
        Self {
            table: Table::default(),
        }
    }
}

impl Display for OwnedSequence {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let borrowed = Sequence::from(self);
        borrowed.fmt(f)
    }
}

impl Default for OwnedSequence {
    fn default() -> Self {
        Self {
            table: OwnedTable::default(),
        }
    }
}

impl<'a> From<Sequence<'a>> for OwnedSequence {
    fn from(sequence: Sequence<'a>) -> Self {
        Self {
            table: OwnedTable::from(sequence.table),
        }
    }
}

impl<'a> From<&'a OwnedSequence> for Sequence<'a> {
    fn from(owned: &'a OwnedSequence) -> Self {
        Self {
            table: Table::from(&owned.table),
        }
    }
}

impl From<OwnedTable> for OwnedSequence {
    fn from(table: OwnedTable) -> Self {
        Self { table }
    }
}

impl<'a> Sequence<'a> {
    /// Convert this borrowed Sequence to an owned OwnedSequence
    pub fn to_owned(&self) -> OwnedSequence {
        OwnedSequence::from(*self)
    }

    /// Generate a setval statement to set the sequence to the max value of the given column
    pub fn setval_from_column(&self, column: &Column<'a>) -> Result<String, Error> {
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
    use pg_query::{parse, NodeEnum};

    use super::{Column, Sequence, Table};

    #[test]
    fn test_sequence_setval_from_alter_statement() {
        let query =
            parse("ALTER SEQUENCE public.user_profiles_id_seq OWNED BY public.user_profiles.id")
                .unwrap();
        let alter = query.protobuf.stmts.first().unwrap().stmt.as_ref().unwrap();

        match alter.node {
            Some(NodeEnum::AlterSeqStmt(ref stmt)) => {
                // Extract sequence name from the relation
                let sequence_table = Table::from(stmt.sequence.as_ref().unwrap());
                let sequence = Sequence::from(sequence_table);

                // Extract column from the owned_by option
                if let Some(node) = stmt.options.first() {
                    let column = Column::try_from(node).unwrap();

                    // Test the setval generation
                    let setval_sql = sequence.setval_from_column(&column).unwrap();

                    assert_eq!(
                        setval_sql,
                        "SELECT setval('\"public\".\"user_profiles_id_seq\"', COALESCE((SELECT MAX(\"id\") FROM \"public\".\"user_profiles\"), 1), true);"
                    );

                    // Verify the individual components
                    assert_eq!(sequence.table.name, "user_profiles_id_seq");
                    assert_eq!(sequence.table.schema, Some("public"));
                    assert_eq!(column.name, "id");
                    assert_eq!(column.table, Some("user_profiles"));
                    assert_eq!(column.schema, Some("public"));
                } else {
                    panic!("no owned by clause");
                }
            }
            _ => panic!("not an alter sequence"),
        }
    }

    #[test]
    fn test_sequence_display() {
        let table = Table {
            name: "my_seq",
            schema: Some("public"),
        };
        let sequence = Sequence::from(table);

        assert_eq!(sequence.to_string(), "\"public\".\"my_seq\"");
    }

    #[test]
    fn test_sequence_display_no_schema() {
        let table = Table {
            name: "my_seq",
            schema: None,
        };
        let sequence = Sequence::from(table);

        assert_eq!(sequence.to_string(), "\"my_seq\"");
    }
}
