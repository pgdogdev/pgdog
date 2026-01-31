//! Schema information for creating foreign tables via postgres_fdw.

mod custom_types;
mod error;
mod extensions;
mod schema;
mod statement;

#[cfg(test)]
mod test;

pub use custom_types::{CustomType, CustomTypeKind, CustomTypes, CUSTOM_TYPES_QUERY};
pub use error::Error;
pub use extensions::{Extension, Extensions, EXTENSIONS_QUERY};
pub use schema::{FdwServerDef, ForeignTableColumn, ForeignTableSchema, FOREIGN_TABLE_SCHEMA};
pub use statement::{
    create_foreign_table, create_foreign_table_with_children, CreateForeignTableResult,
    ForeignTableBuilder, PartitionStrategy, TypeMismatch,
};

pub(crate) use statement::quote_identifier;
