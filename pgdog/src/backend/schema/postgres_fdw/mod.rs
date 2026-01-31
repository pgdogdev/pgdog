//! Schema information for creating foreign tables via postgres_fdw.

mod schema;
mod statement;

pub use schema::{ForeignTableColumn, FOREIGN_TABLE_SCHEMA};
pub use statement::{create_foreign_table, ForeignTableBuilder, PartitionStrategy};
