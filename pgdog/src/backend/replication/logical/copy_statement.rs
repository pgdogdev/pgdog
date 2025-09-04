//!
//! Generate COPY statement for table synchronization.
//!

/// COPY statement generator.
#[derive(Debug, Clone)]
pub(crate) struct CopyStatement {
    schema: String,
    table: String,
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
    pub(crate) fn new(schema: &str, table: &str, columns: &[String]) -> CopyStatement {
        CopyStatement {
            schema: schema.to_owned(),
            table: table.to_owned(),
            columns: columns.to_vec(),
        }
    }

    /// Generate COPY ... TO STDOUT statement.
    pub(crate) fn copy_out(&self) -> String {
        self.copy(true)
    }

    /// Generate COPY ... FROM STDIN statement.
    pub(crate) fn copy_in(&self) -> String {
        self.copy(false)
    }

    // Generate the statement.
    fn copy(&self, out: bool) -> String {
        format!(
            r#"COPY "{}"."{}" ({}) {} WITH (FORMAT binary)"#,
            self.schema,
            self.table,
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
        let copy = CopyStatement::new("public", "test", &["id".into(), "email".into()]).copy_in();
        assert_eq!(
            copy.to_string(),
            r#"COPY "public"."test" ("id", "email") FROM STDIN WITH (FORMAT binary)"#
        );

        let copy = CopyStatement::new("public", "test", &["id".into(), "email".into()]).copy_out();
        assert_eq!(
            copy.to_string(),
            r#"COPY "public"."test" ("id", "email") TO STDOUT WITH (FORMAT binary)"#
        );
    }
}
