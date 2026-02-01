//! Schema information for creating foreign tables via postgres_fdw.

mod error;
mod schema;
mod statement;

pub use error::Error;
pub use schema::{ForeignTableColumn, ForeignTableSchema, FOREIGN_TABLE_SCHEMA};
pub use statement::{create_foreign_table, ForeignTableBuilder, PartitionStrategy};

pub(crate) use statement::quote_identifier;
