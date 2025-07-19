use std::fmt::Display;

#[derive(Debug, Clone)]
pub struct CopyStatement {
    schema: String,
    table: String,
    columns: Vec<String>,
    out: bool,
}

impl CopyStatement {
    pub fn new_out(schema: &str, table: &str, columns: &[String]) -> CopyStatement {
        CopyStatement {
            schema: schema.to_owned(),
            table: table.to_owned(),
            columns: columns.to_vec(),
            out: true,
        }
    }

    pub fn new_in(schema: &str, table: &str, columns: &[String]) -> CopyStatement {
        let mut in_ = Self::new_out(schema, table, columns);
        in_.out = false;

        in_
    }
}

impl Display for CopyStatement {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            r#"COPY "{}"."{}" ({}) "#,
            self.schema,
            self.table,
            self.columns
                .iter()
                .map(|c| format!(r#""{}""#, c))
                .collect::<Vec<_>>()
                .join(", ")
        )?;

        if self.out {
            write!(f, "TO STDOUT")
        } else {
            write!(f, "FROM STDIN")
        }
    }
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn test_copy_stmt() {
        let copy = CopyStatement::new_in("public", "test", &["id".into(), "email".into()]);
        assert_eq!(
            copy.to_string(),
            r#"COPY "public"."test" ("id", "email") FROM STDIN"#
        );

        let copy = CopyStatement::new_out("public", "test", &["id".into(), "email".into()]);
        assert_eq!(
            copy.to_string(),
            r#"COPY "public"."test" ("id", "email") TO STDOUT"#
        );
    }
}
