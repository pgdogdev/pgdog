//!
//! Generate COPY statement for table synchronization.
//!

use super::data_sync::source_regclass;
use super::publisher::Table;
use pgdog_config::CopyFormat;

use crate::backend::{
    ConnectReason, Server, ServerOptions, pool::Address, replication::logical::Error,
};

use super::publisher::PublicationTable;

/// Sends a SELECT to Postgres to see the total amount of blocks, so that we can
/// split them into equal ctid ranges to parallelize our table reads without worrying
/// about primary key ranges. Stores the results, and handles the WHERE clause 'generation'
#[derive(Debug, Clone, Default)]
pub(crate) struct TableColumnSplit {
    pub(crate) blocks: u32,
    pub(crate) split_statement_count: usize,
}

impl TableColumnSplit {
    pub(crate) async fn try_new(
        table: &Table,
        split_statement_count: usize,
        address: &Address,
    ) -> Result<Self, Error> {
        Ok(Self {
            blocks: Self::fetch_blocks(table, address).await?,
            split_statement_count,
        })
    }

    /// Represents the subselect range (WHERE clause) used within the COPY OUT statement
    /// for each worker (for the `index_of_worker` worker)
    ///
    /// This uses ctid instead of a primary key range to ensure
    /// contiguous reads and allow us to work with
    /// different primary key types (e.g., UUID, strings) which would
    /// otehrwise be hard to split across evenly
    fn get_range_modifier(&self, index_of_worker: usize) -> String {
        let split_count = self.split_statement_count;

        let (min_tid_block_number, max_tid_block_number) = {
            let subsec = self.blocks / self.split_statement_count as u32;

            let local_min = subsec * index_of_worker as u32;
            let local_max = local_min + subsec;

            (local_min, local_max)
        };

        let (min_tid_fmt, max_tid_fmt) = (
            format!("'({min_tid_block_number}, 0)'::tid"),
            format!("'({max_tid_block_number}, 0)'::tid"),
        );

        match index_of_worker {
            0 => format!("ctid < {max_tid_fmt} AND ctid >= {min_tid_fmt}"),
            last if last == split_count - 1 => format!("ctid >= {min_tid_fmt}"),
            // inner ones (btwn min and max index)
            _ => format!("ctid >= {min_tid_fmt} AND ctid < {max_tid_fmt}"),
        }
    }

    async fn fetch_blocks(table: &Table, address: &Address) -> Result<u32, Error> {
        // While we could fetch the max(ctid), that would incur a traverse over
        // the entire table (O(n))
        //
        // This side-steps that with an O(1) fetch via the on-disk size of the table divided
        // by the disk block size (default 8KB). This will be equal to the amount of ctid blocks
        // that max would've returned.
        let sql = format!(
            "SELECT pg_relation_size({}::regclass) / current_setting('block_size')::bigint",
            source_regclass(table)
        );

        let mut server = Server::connect(
            address,
            ServerOptions::default(),
            ConnectReason::Resharding,
            Default::default(),
        )
        .await?;

        let blocks: i64 = server
            .fetch_all(sql)
            .await?
            .pop()
            .ok_or(Error::MissingData)?;

        u32::try_from(blocks).map_err(|_| Error::MissingData)
    }
}

/// COPY statement generator.
#[derive(Debug, Clone)]
pub(crate) struct CopyStatement {
    table: PublicationTable,
    columns: Vec<String>,
    copy_format: CopyFormat,
    table_col_split: TableColumnSplit,
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
    pub(crate) fn new(
        table: &PublicationTable,
        columns: &[String],
        copy_format: CopyFormat,
        table_col_split: TableColumnSplit,
    ) -> CopyStatement {
        CopyStatement {
            table: table.clone(),
            columns: columns.to_vec(),
            copy_format,
            table_col_split,
        }
    }

    /// Generate COPY ... TO STDOUT statement.
    pub(crate) fn copy_out(&self, index: usize) -> String {
        if self.table_col_split.split_statement_count > 1 {
            self.copy_table_out_parallel(index)
        } else {
            self.copy(true)
        }
    }

    /// Generate COPY ... FROM STDIN statement.
    pub(crate) fn copy_in(&self) -> String {
        self.copy(false)
    }

    fn schema_name(&self, out: bool) -> &str {
        if out || self.table.parent_schema.is_empty() {
            &self.table.schema
        } else {
            &self.table.parent_schema
        }
    }

    fn table_name(&self, out: bool) -> &str {
        if out || self.table.parent_name.is_empty() {
            &self.table.name
        } else {
            &self.table.parent_name
        }
    }

    /// Generate a serial COPY statement.
    fn copy(&self, out: bool) -> String {
        format!(
            r#"COPY "{}"."{}" ({}) {} WITH (FORMAT {})"#,
            self.schema_name(out),
            self.table_name(out),
            self.columns
                .iter()
                .map(|c| format!(r#""{}""#, c))
                .collect::<Vec<_>>()
                .join(", "),
            if out { "TO STDOUT" } else { "FROM STDIN" },
            self.copy_format
        )
    }

    /// Generate a COPY TO STDOUT statement in the parallel
    /// format, using a special WHERE clause generated based on
    /// the ctid range fetched earlier from `TableColSplit`
    fn copy_table_out_parallel(&self, index: usize) -> String {
        let range_modifier = self.table_col_split.get_range_modifier(index);
        let stmt_out = format!(
            r#"COPY (SELECT {} FROM "{}"."{}" WHERE {}) TO STDOUT WITH (FORMAT {})"#,
            self.columns
                .iter()
                .map(|c| format!(r#""{}""#, c))
                .collect::<Vec<_>>()
                .join(", "),
            self.schema_name(true),
            self.table_name(true),
            range_modifier,
            self.copy_format
        );

        stmt_out
    }
}

#[cfg(test)]
mod test {
    use crate::backend::replication::publisher::test::{
        PublicationTest, setup_publication_table_with_data_type_identity_col,
    };

    use super::*;

    /// Verify that the 3 'range modifiers' (WHERE clauses) are generated
    /// as we'd like using the correct ctid ranges when dealing with
    /// parallel table COPYs.
    ///
    /// Tests using 1 parallel read, 2 parallel reads, and 3 parallel reads
    /// to check all formats.
    #[tokio::test]
    async fn test_parallel_range_copy_statement_gen() {
        const PUBLICATION_NAME: &str = "test_pub";

        let mut test: PublicationTest = setup_publication_table_with_data_type_identity_col(
            PUBLICATION_NAME,
            "test_table",
            "BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY",
        )
        .await;

        let tables: Vec<Table> = Table::load(PUBLICATION_NAME, &mut test.server)
            .await
            .unwrap();

        let table = tables.first().unwrap();

        for split_statement_count in [1, 2, 3] {
            let address = &Address::new_test();
            let table_col_split = TableColumnSplit::try_new(table, split_statement_count, address)
                .await
                .unwrap();

            let copy = CopyStatement::new(
                &table.table,
                &["id".into(), "text".into()],
                CopyFormat::Binary,
                table_col_split,
            );

            if split_statement_count == 1 {
                assert_eq!(
                    copy.copy_out(0),
                    "COPY \"pgdog\".\"test_table\" (\"id\", \"text\") TO STDOUT WITH (FORMAT binary)",
                    "{}",
                    copy.copy_out(0)
                );
            } else if split_statement_count == 2 {
                assert!(
                    copy.copy_out(0)
                        .contains("WHERE ctid < '(368, 0)'::tid AND ctid >= '(0, 0)'::tid"),
                    "{}",
                    copy.copy_out(0)
                );
                assert!(
                    copy.copy_out(1).contains("WHERE ctid >= '(368, 0)'::tid"),
                    "{}",
                    copy.copy_out(1)
                );
            } else if split_statement_count == 3 {
                assert!(
                    copy.copy_out(0)
                        .contains("WHERE ctid < '(245, 0)'::tid AND ctid >= '(0, 0)'::tid"),
                    "{}",
                    copy.copy_out(0)
                );

                assert!(
                    copy.copy_out(1)
                        .contains("WHERE ctid >= '(245, 0)'::tid AND ctid < '(490, 0)'::tid"),
                    "{}",
                    copy.copy_out(1)
                );

                assert!(
                    copy.copy_out(2).contains("WHERE ctid >= '(490, 0)'::tid"),
                    "{}",
                    copy.copy_out(2)
                );
            }
        }
    }

    #[test]
    fn test_copy_stmt() {
        let table = PublicationTable {
            schema: "public".into(),
            name: "test".into(),
            ..Default::default()
        };

        let columns = ["id".into(), "email".into()];
        let split = TableColumnSplit::default();

        let copy = CopyStatement::new(&table, &columns, CopyFormat::Binary, split.clone());
        let copy_in = copy.copy_in();
        assert_eq!(
            copy_in,
            r#"COPY "public"."test" ("id", "email") FROM STDIN WITH (FORMAT binary)"#
        );

        assert_eq!(
            copy.copy_out(0),
            r#"COPY "public"."test" ("id", "email") TO STDOUT WITH (FORMAT binary)"#
        );

        let table = PublicationTable {
            schema: "public".into(),
            name: "test_0".into(),
            parent_name: "test".into(),
            parent_schema: "public".into(),
            ..Default::default()
        };

        let copy = CopyStatement::new(&table, &columns, CopyFormat::Binary, split.clone());
        let copy_in = copy.copy_in();
        assert_eq!(
            copy_in,
            r#"COPY "public"."test" ("id", "email") FROM STDIN WITH (FORMAT binary)"#
        );

        assert_eq!(
            copy.copy_out(0),
            r#"COPY "public"."test_0" ("id", "email") TO STDOUT WITH (FORMAT binary)"#
        );
    }
}
