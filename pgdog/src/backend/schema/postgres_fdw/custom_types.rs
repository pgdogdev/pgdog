//! Custom type definitions (enums, domains, composite types) for foreign tables.

use std::fmt::Write;

use crate::net::messages::DataRow;

use super::quote_identifier;

/// Query to fetch custom type definitions.
pub static CUSTOM_TYPES_QUERY: &str = include_str!("custom_types.sql");

/// Kind of custom type.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CustomTypeKind {
    Enum,
    Domain,
    Composite,
}

impl CustomTypeKind {
    fn from_str(s: &str) -> Option<Self> {
        match s {
            "enum" => Some(Self::Enum),
            "domain" => Some(Self::Domain),
            "composite" => Some(Self::Composite),
            _ => None,
        }
    }
}

/// A custom type definition from the database.
#[derive(Debug, Clone)]
pub struct CustomType {
    pub kind: CustomTypeKind,
    pub schema_name: String,
    pub type_name: String,
    /// Base type for domains.
    pub base_type: String,
    /// Constraint definition for domains (e.g., "CHECK (VALUE > 0)").
    pub constraint_def: String,
    /// Default value for domains.
    pub default_value: String,
    /// Collation name.
    pub collation_name: String,
    /// Collation schema.
    pub collation_schema: String,
    /// Comma-separated enum labels.
    pub enum_labels: String,
    /// Comma-separated composite attributes (name type pairs).
    pub composite_attributes: String,
}

impl From<DataRow> for CustomType {
    fn from(value: DataRow) -> Self {
        let kind_str = value.get_text(0).unwrap_or_default();
        Self {
            kind: CustomTypeKind::from_str(&kind_str).unwrap_or(CustomTypeKind::Enum),
            schema_name: value.get_text(1).unwrap_or_default(),
            type_name: value.get_text(2).unwrap_or_default(),
            base_type: value.get_text(3).unwrap_or_default(),
            constraint_def: value.get_text(4).unwrap_or_default(),
            default_value: value.get_text(5).unwrap_or_default(),
            collation_name: value.get_text(6).unwrap_or_default(),
            collation_schema: value.get_text(7).unwrap_or_default(),
            enum_labels: value.get_text(8).unwrap_or_default(),
            composite_attributes: value.get_text(9).unwrap_or_default(),
        }
    }
}

impl CustomType {
    /// Fully qualified type name.
    pub fn qualified_name(&self) -> String {
        format!(
            "{}.{}",
            quote_identifier(&self.schema_name),
            quote_identifier(&self.type_name)
        )
    }

    /// Generate the CREATE statement for this type.
    pub fn create_statement(&self) -> Result<String, super::Error> {
        match self.kind {
            CustomTypeKind::Enum => self.create_enum_statement(),
            CustomTypeKind::Domain => self.create_domain_statement(),
            CustomTypeKind::Composite => self.create_composite_statement(),
        }
    }

    fn create_enum_statement(&self) -> Result<String, super::Error> {
        let mut sql = String::new();
        write!(sql, "CREATE TYPE {} AS ENUM (", self.qualified_name())?;

        let labels: Vec<&str> = self.enum_labels.split(',').collect();
        for (i, label) in labels.iter().enumerate() {
            if i > 0 {
                sql.push_str(", ");
            }
            write!(sql, "'{}'", label.replace('\'', "''"))?;
        }

        sql.push(')');
        Ok(sql)
    }

    fn create_domain_statement(&self) -> Result<String, super::Error> {
        let mut sql = String::new();
        write!(
            sql,
            "CREATE DOMAIN {} AS {}",
            self.qualified_name(),
            self.base_type
        )?;

        if self.has_collation() {
            write!(
                sql,
                " COLLATE {}.{}",
                quote_identifier(&self.collation_schema),
                quote_identifier(&self.collation_name)
            )?;
        }

        if !self.default_value.is_empty() {
            write!(sql, " DEFAULT {}", self.default_value)?;
        }

        if !self.constraint_def.is_empty() {
            write!(sql, " {}", self.constraint_def)?;
        }

        Ok(sql)
    }

    fn create_composite_statement(&self) -> Result<String, super::Error> {
        let mut sql = String::new();
        write!(sql, "CREATE TYPE {} AS (", self.qualified_name())?;

        // Split on newlines since type definitions can contain commas
        let attrs: Vec<&str> = self.composite_attributes.split('\n').collect();
        for (i, attr) in attrs.iter().enumerate() {
            if i > 0 {
                sql.push_str(", ");
            }
            let attr = attr.trim();
            if let Some((name, typ)) = attr.split_once(' ') {
                write!(sql, "{} {}", quote_identifier(name), typ)?;
            } else {
                sql.push_str(attr);
            }
        }

        sql.push(')');
        Ok(sql)
    }

    fn has_collation(&self) -> bool {
        !self.collation_name.is_empty() && !self.collation_schema.is_empty()
    }
}

/// Collection of custom types from a database.
#[derive(Debug, Clone, Default)]
pub struct CustomTypes {
    types: Vec<CustomType>,
}

impl CustomTypes {
    /// Load custom types from a server.
    pub(crate) async fn load(
        server: &mut crate::backend::Server,
    ) -> Result<Self, crate::backend::Error> {
        let types: Vec<CustomType> = server.fetch_all(CUSTOM_TYPES_QUERY).await?;
        Ok(Self { types })
    }

    /// Create all custom types on the target server.
    pub(crate) async fn setup(
        &self,
        server: &mut crate::backend::Server,
    ) -> Result<(), crate::backend::Error> {
        for custom_type in &self.types {
            let stmt = custom_type.create_statement()?;
            tracing::debug!("[fdw::setup] {} [{}]", stmt, server.addr());
            server.execute(&stmt).await?;
        }

        Ok(())
    }

    /// Get the types.
    pub fn types(&self) -> &[CustomType] {
        &self.types
    }

    /// Check if there are any custom types.
    pub fn is_empty(&self) -> bool {
        self.types.is_empty()
    }
}

#[cfg(test)]
mod test {
    use super::*;

    fn test_enum() -> CustomType {
        CustomType {
            kind: CustomTypeKind::Enum,
            schema_name: "core".into(),
            type_name: "user_status".into(),
            base_type: String::new(),
            constraint_def: String::new(),
            default_value: String::new(),
            collation_name: String::new(),
            collation_schema: String::new(),
            enum_labels: "active,inactive,suspended".into(),
            composite_attributes: String::new(),
        }
    }

    fn test_domain() -> CustomType {
        CustomType {
            kind: CustomTypeKind::Domain,
            schema_name: "core".into(),
            type_name: "email".into(),
            base_type: "character varying(255)".into(),
            constraint_def: "CHECK ((VALUE)::text ~ '^[A-Za-z0-9._%+-]+@'::text)".into(),
            default_value: String::new(),
            collation_name: String::new(),
            collation_schema: String::new(),
            enum_labels: String::new(),
            composite_attributes: String::new(),
        }
    }

    fn test_composite() -> CustomType {
        CustomType {
            kind: CustomTypeKind::Composite,
            schema_name: "core".into(),
            type_name: "geo_point".into(),
            base_type: String::new(),
            constraint_def: String::new(),
            default_value: String::new(),
            collation_name: String::new(),
            collation_schema: String::new(),
            enum_labels: String::new(),
            composite_attributes: "latitude numeric(9,6)\nlongitude numeric(9,6)".into(),
        }
    }

    #[test]
    fn test_create_enum_statement() {
        let t = test_enum();
        let sql = t.create_statement().unwrap();
        assert_eq!(
            sql,
            r#"CREATE TYPE "core"."user_status" AS ENUM ('active', 'inactive', 'suspended')"#
        );
    }

    #[test]
    fn test_create_domain_statement() {
        let t = test_domain();
        let sql = t.create_statement().unwrap();
        assert!(sql.contains(r#"CREATE DOMAIN "core"."email" AS character varying(255)"#));
        assert!(sql.contains("CHECK"));
    }

    #[test]
    fn test_create_composite_statement() {
        let t = test_composite();
        let sql = t.create_statement().unwrap();
        assert!(sql.contains(r#"CREATE TYPE "core"."geo_point" AS ("#));
        assert!(sql.contains(r#""latitude" numeric(9,6)"#));
        assert!(sql.contains(r#""longitude" numeric(9,6)"#));
    }
}
