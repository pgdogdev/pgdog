use std::fmt::Display;

#[derive(Debug, Clone)]
pub struct CopyStatement {
    schema: String,
    table: String,
    columns: Vec<String>,
    out: bool,
}

impl CopyStatement {
    pub fn new(schema: &str, table: &str, columns: &[String]) -> CopyStatement {
        CopyStatement {
            schema: schema.to_owned(),
            table: table.to_owned(),
            columns: columns.to_vec(),
            out: true,
        }
    }

    pub fn copy_out(mut self) -> Self {
        self.out = true;
        self
    }

    pub fn copy_in(mut self) -> Self {
        self.out = false;
        self
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
        let copy = CopyStatement::new("public", "test", &["id".into(), "email".into()]).copy_in();
        assert_eq!(
            copy.to_string(),
            r#"COPY "public"."test" ("id", "email") FROM STDIN"#
        );

        let copy = CopyStatement::new("public", "test", &["id".into(), "email".into()]).copy_out();
        assert_eq!(
            copy.to_string(),
            r#"COPY "public"."test" ("id", "email") TO STDOUT"#
        );
    }
}
