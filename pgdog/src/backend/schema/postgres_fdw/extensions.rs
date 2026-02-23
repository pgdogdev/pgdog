//! Extension definitions for foreign tables.

use std::fmt::Write;

use crate::net::messages::DataRow;

use super::quote_identifier;

/// Query to fetch installed extensions.
pub static EXTENSIONS_QUERY: &str = include_str!("extensions.sql");

/// An installed extension.
#[derive(Debug, Clone)]
pub struct Extension {
    pub name: String,
    pub schema_name: String,
    pub version: String,
}

impl From<DataRow> for Extension {
    fn from(value: DataRow) -> Self {
        Self {
            name: value.get_text(0).unwrap_or_default(),
            schema_name: value.get_text(1).unwrap_or_default(),
            version: value.get_text(2).unwrap_or_default(),
        }
    }
}

impl Extension {
    /// Generate the CREATE EXTENSION statement.
    pub fn create_statement(&self) -> Result<String, super::Error> {
        let mut sql = String::new();
        write!(
            sql,
            "CREATE EXTENSION IF NOT EXISTS {}",
            quote_identifier(&self.name)
        )?;

        // Only specify schema if it's not the default 'public'
        if !self.schema_name.is_empty() && self.schema_name != "public" {
            write!(sql, " SCHEMA {}", quote_identifier(&self.schema_name))?;
        }

        Ok(sql)
    }
}

/// Collection of extensions from a database.
#[derive(Debug, Clone, Default)]
pub struct Extensions {
    extensions: Vec<Extension>,
}

impl Extensions {
    /// Load extensions from a server.
    pub(crate) async fn load(
        server: &mut crate::backend::Server,
    ) -> Result<Self, crate::backend::Error> {
        let extensions: Vec<Extension> = server.fetch_all(EXTENSIONS_QUERY).await?;
        Ok(Self { extensions })
    }

    /// Create all extensions on the target server.
    pub(crate) async fn setup(
        &self,
        server: &mut crate::backend::Server,
    ) -> Result<(), crate::backend::Error> {
        for ext in &self.extensions {
            let stmt = ext.create_statement()?;
            tracing::debug!("[fdw::setup] {} [{}]", stmt, server.addr());
            server.execute(&stmt).await?;
        }

        Ok(())
    }

    /// Get the extensions.
    pub fn extensions(&self) -> &[Extension] {
        &self.extensions
    }

    /// Check if there are any extensions.
    pub fn is_empty(&self) -> bool {
        self.extensions.is_empty()
    }
}

#[cfg(test)]
mod test {
    use super::*;

    fn test_extension() -> Extension {
        Extension {
            name: "ltree".into(),
            schema_name: "public".into(),
            version: "1.2".into(),
        }
    }

    fn test_extension_with_schema() -> Extension {
        Extension {
            name: "pg_trgm".into(),
            schema_name: "extensions".into(),
            version: "1.6".into(),
        }
    }

    #[test]
    fn test_create_extension_statement() {
        let ext = test_extension();
        let sql = ext.create_statement().unwrap();
        assert_eq!(sql, r#"CREATE EXTENSION IF NOT EXISTS "ltree""#);
    }

    #[test]
    fn test_create_extension_statement_with_schema() {
        let ext = test_extension_with_schema();
        let sql = ext.create_statement().unwrap();
        assert_eq!(
            sql,
            r#"CREATE EXTENSION IF NOT EXISTS "pg_trgm" SCHEMA "extensions""#
        );
    }
}
