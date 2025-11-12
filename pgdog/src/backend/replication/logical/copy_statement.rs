//!
//! Generate COPY statement for table synchronization.
//!

use super::publisher::PublicationTable;

/// COPY statement generator.
#[derive(Debug, Clone)]
pub struct CopyStatement {
    table: PublicationTable,
    columns: Vec<String>,
}

impl CopyStatement {
    /// Create new COPY statement generator.
    ///
    /// # Arguments
    ///
    /// * `schema`: Name of the schema.
    /// * `table`: Name of the table.
    /// * `columns`: Table column names.
    ///
    pub fn new(table: &PublicationTable, columns: &[String]) -> CopyStatement {
        CopyStatement {
            table: table.clone(),
            columns: columns.to_vec(),
        }
    }

    /// Generate COPY ... TO STDOUT statement.
    pub fn copy_out(&self) -> String {
        self.copy(true)
    }

    /// Generate COPY ... FROM STDIN statement.
    pub fn copy_in(&self) -> String {
        self.copy(false)
    }

    fn schema_name(&self, out: bool) -> &str {
        if out {
            &self.table.schema
        } else {
            if self.table.parent_schema.is_empty() {
                &self.table.schema
            } else {
                &self.table.parent_schema
            }
        }
    }

    fn table_name(&self, out: bool) -> &str {
        if out {
            &self.table.name
        } else {
            if self.table.parent_name.is_empty() {
                &self.table.name
            } else {
                &self.table.parent_name
            }
        }
    }

    // Generate the statement.
    fn copy(&self, out: bool) -> String {
        format!(
            r#"COPY "{}"."{}" ({}) {} WITH (FORMAT binary)"#,
            self.schema_name(out),
            self.table_name(out),
            self.columns
                .iter()
                .map(|c| format!(r#""{}""#, c))
                .collect::<Vec<_>>()
                .join(", "),
            if out { "TO STDOUT" } else { "FROM STDIN" }
        )
    }
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn test_copy_stmt() {
        let table = PublicationTable {
            schema: "public".into(),
            name: "test".into(),
            ..Default::default()
        };

        let copy = CopyStatement::new(&table, &["id".into(), "email".into()]);
        let copy_in = copy.copy_in();
        assert_eq!(
            copy_in,
            r#"COPY "public"."test" ("id", "email") FROM STDIN WITH (FORMAT binary)"#
        );

        assert_eq!(
            copy.copy_out(),
            r#"COPY "public"."test" ("id", "email") TO STDOUT WITH (FORMAT binary)"#
        );

        let table = PublicationTable {
            schema: "public".into(),
            name: "test_0".into(),
            parent_name: "test".into(),
            parent_schema: "public".into(),
            ..Default::default()
        };

        let copy = CopyStatement::new(&table, &["id".into(), "email".into()]);
        let copy_in = copy.copy_in();
        assert_eq!(
            copy_in,
            r#"COPY "public"."test" ("id", "email") FROM STDIN WITH (FORMAT binary)"#
        );

        assert_eq!(
            copy.copy_out(),
            r#"COPY "public"."test_0" ("id", "email") TO STDOUT WITH (FORMAT binary)"#
        );
    }
}
