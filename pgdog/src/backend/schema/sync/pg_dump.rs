//! Schema migration via `pg_dump`.
//!
//! Dumps the source, rewrites it for a sharded destination, then applies the
//! statements for a given [`SyncState`] phase to every destination shard.
//!
//! Always dump the whole schema, never `pg_dump --section` since it could
//! be missing some important data that is required to make the sharding work
//! e.g. foreign keys widening to `bigint` and others.

use std::{
    collections::{HashMap, HashSet},
    fmt::Display,
    str::from_utf8,
};

use lazy_static::lazy_static;
use pg_raw_parse::{Node, NodeMut, Owned, StmtList, make, nodes};
use regex::Regex;
use tracing::{info, trace, warn};

use super::SchemaSyncError;
use crate::{
    backend::{self, Cluster, pool::Request, replication::publisher::PublicationTable},
    config::config,
    frontend::router::parser::{Column, Sequence, Table},
};

/// Key for looking up column types during pg_dump parsing.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
struct ColumnTypeKey<'a> {
    schema: &'a str,
    table: &'a str,
    column: &'a str,
}

fn schema_name(relation: &nodes::RangeVar) -> &str {
    relation.schemaname().unwrap_or("public")
}

fn is_integer_type(type_name: &str) -> bool {
    matches!(
        type_name,
        "int4" | "int2" | "serial" | "smallserial" | "integer" | "smallint"
    )
}

/// Determine if a column should be converted to bigint.
///
/// A column should be converted if:
/// 1. It's in the columns_to_convert set (PK/FK that references integer PK), OR
/// 2. It's a child partition column where the parent has bigint and child has integer
fn should_convert_to_bigint<'a>(
    col: &Column<'a>,
    col_type_name: Option<&nodes::TypeName>,
    columns_to_convert: &HashSet<Column<'a>>,
    parent_table: Option<&Table<'a>>,
    parent_column_types: &HashMap<(Table<'a>, &str), &'static str>,
) -> bool {
    // Check if this column is directly marked for conversion (PK/FK)
    if columns_to_convert.contains(col) {
        return true;
    }

    // Check if this is a child partition where parent has bigint
    let Some(parent) = parent_table else {
        return false;
    };

    let Some(&parent_type) = parent_column_types.get(&(*parent, col.name)) else {
        return false;
    };

    if parent_type != "int8" {
        return false;
    }

    // Parent has bigint, check if child has integer type
    let Some(type_name) = col_type_name
        .and_then(|n| n.names().iter().next_back())
        .and_then(|s| s.sval())
    else {
        return false;
    };

    is_integer_type(type_name)
}

use tokio::process::Command;

/// A pending dump of one source cluster's schema. Every [`PgDump::dump`]
/// refetches the schema again
#[derive(Debug, Clone)]
pub(crate) struct PgDump {
    source: Cluster,
    publication: String,
}

fn build_pg_dump_command(
    pg_dump_path: &str,
    addr: &backend::pool::Address,
    auth_secret: &str,
) -> Command {
    let mut command = Command::new(pg_dump_path);
    command
        .kill_on_drop(true)
        .arg("--schema-only")
        .arg("-h")
        .arg(&addr.host)
        .arg("-p")
        .arg(addr.port.to_string())
        .arg("-U")
        .arg(&addr.user)
        .env("PGPASSWORD", auth_secret)
        .arg("-d")
        .arg(&addr.database_name);

    if addr.server_auth.is_external_identity() {
        command.env("PGSSLMODE", "require");
    }

    command
}

impl PgDump {
    pub(crate) fn new(source: &Cluster, publication: &str) -> Self {
        Self {
            source: source.clone(),
            publication: publication.to_string(),
        }
    }

    fn clean(source: &str) -> String {
        lazy_static! {
            static ref CLEANUP_RE: Regex = Regex::new(r"(?m)^\\(?:un)?restrict.*\n?").unwrap();
        }
        let cleaned = CLEANUP_RE.replace_all(source, "");

        cleaned.to_string()
    }

    /// Dump the source schema.
    ///
    /// Cross-checks the publication's table list on every shard, then runs
    /// `pg_dump --schema-only` against shard 0 only: all shards are assumed to
    /// share a schema, so a mismatch only warns.
    pub(crate) async fn dump(&self) -> Result<PgDumpOutput, SchemaSyncError> {
        let mut comparison: Vec<PublicationTable> = vec![];
        let addr = self
            .source
            .shards()
            .first()
            .ok_or(SchemaSyncError::NoDatabases)?
            .pools()
            .first()
            .ok_or(SchemaSyncError::NoDatabases)?
            .addr()
            .clone();

        info!(
            "loading tables from publication \"{}\" on {} shards [{}]",
            self.publication,
            self.source.shards().len(),
            self.source.name(),
        );

        for (num, shard) in self.source.shards().iter().enumerate() {
            let mut server = shard.primary_or_replica(&Request::default()).await?;
            let tables = PublicationTable::load(&self.publication, &mut server).await?;
            if comparison.is_empty() {
                comparison.extend(tables);
            } else if comparison != tables {
                warn!(
                    "shard {} tables are different [{}, {}]",
                    num,
                    server.addr(),
                    self.source.name()
                );
            }
        }

        if comparison.is_empty() {
            return Err(SchemaSyncError::PublicationNoTables(
                self.publication.clone(),
            ));
        }

        info!("dumping schema [{}, {}]", comparison.len(), addr,);

        let config = config();
        let pg_dump_path = config
            .config
            .replication
            .pg_dump_path
            .to_str()
            .unwrap_or("pg_dump");

        let auth_secret = addr.auth_secrets().await?;
        let mut command = build_pg_dump_command(
            pg_dump_path,
            &addr,
            auth_secret.first().ok_or(SchemaSyncError::PgDump(
                "server has no configured passwords".into(),
            ))?,
        );
        let output = command.output().await?;

        if !output.status.success() {
            let err = from_utf8(&output.stderr)?;
            return Err(SchemaSyncError::PgDump(err.to_string()));
        }

        let original = from_utf8(&output.stdout)?.to_string();
        trace!("[pg_dump (original)] {}", original);

        let cleaned = Self::clean(&original);
        trace!("[pg_dump (clean)] {}", cleaned);

        let stmts = pg_raw_parse::parse(&cleaned)?.into_inner();

        Ok(PgDumpOutput {
            stmts,
            original: cleaned,
        })
    }
}

/// A parsed pg_dump output that every phase of one migration can reuse.
#[derive(Debug)]
pub(crate) struct PgDumpOutput {
    stmts: Owned<StmtList>,
    /// The cleaned SQL text the parse came from. [`PgDumpOutput::statements`]
    /// slices it by each statement's location to recover source verbatim
    /// instead of deparsing, so any change to `clean` must preserve the byte
    /// offsets the parser saw.
    original: String,
}

pub(crate) use pgdog_stats::SchemaSyncStatement as Statement;
pub(crate) use pgdog_stats::SyncState;

/// The deparsed SQL is copied out: it lives in a Postgres memory context that
/// is freed when the [`pg_raw_parse::DeparseResult`] drops.
fn deparsed<'n, N: Into<Node<'n>>>(node: N) -> Result<Statement, SchemaSyncError> {
    Ok(Statement::new(pg_raw_parse::deparse(node)?.as_str()))
}

/// SQL longer than this is cut short in the log, with an ellipsis. The full
/// text of every statement is available from `schema-sync --dry-run`.
const LOG_CHARS: usize = 200;

pub(crate) struct Collapsed<'a>(pub(crate) &'a str);

impl Display for Collapsed<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let words = self
            .0
            .lines()
            .map(str::trim)
            .filter(|line| !line.is_empty() && !line.starts_with("--"))
            .flat_map(|line| line.split_whitespace());

        let mut text = String::new();
        for word in words {
            if needs_space(&text, word) {
                text.push(' ');
            }
            text.push_str(word);
        }

        match text.char_indices().nth(LOG_CHARS) {
            Some((cut, _)) => write!(f, "{}…", &text[..cut]),
            None => f.write_str(&text),
        }
    }
}

fn needs_space(text: &str, word: &str) -> bool {
    !text.is_empty() && !text.ends_with(['(', '[']) && !word.starts_with([')', ']', ',', ';'])
}

impl PgDumpOutput {
    /// Get integer primary key columns (columns that are part of PRIMARY KEY
    /// constraints and have integer types like int4, int2, serial, etc.).
    fn integer_primary_key_columns(&self) -> HashSet<Column<'_>> {
        let column_types = self.column_types();
        let mut result = HashSet::new();

        for stmt in self.stmts.stmts() {
            let Node::AlterTableStmt(alter_stmt) = stmt else {
                continue;
            };

            let Some(relation) = alter_stmt.relation() else {
                continue;
            };

            for cmd in alter_stmt.cmds() {
                let Node::AlterTableCmd(cmd) = cmd else {
                    continue;
                };

                if cmd.subtype != nodes::AlterTableType::AT_AddConstraint {
                    continue;
                }

                let Node::Constraint(cons) = cmd.def() else {
                    continue;
                };

                if cons.contype != nodes::ConstrType::CONSTR_PRIMARY {
                    continue;
                }

                let schema = schema_name(relation);
                let table_name = relation.relname().unwrap_or_default();

                for key in cons.keys() {
                    let Some(col_name) = key.as_str() else {
                        continue;
                    };

                    let type_key = ColumnTypeKey {
                        schema,
                        table: table_name,
                        column: col_name,
                    };

                    let is_integer = column_types
                        .get(&type_key)
                        .map(|t| is_integer_type(t))
                        .unwrap_or(false);

                    if is_integer {
                        result.insert(Column {
                            name: col_name,
                            table: Some(table_name),
                            schema: Some(schema),
                        });
                    }
                }
            }
        }

        result
    }

    /// Get integer foreign key columns (FK columns that reference integer PKs).
    fn integer_foreign_key_columns(&self) -> HashSet<Column<'_>> {
        let integer_pks = self.integer_primary_key_columns();
        let mut result = HashSet::new();

        for stmt in self.stmts.stmts() {
            let Node::AlterTableStmt(alter_stmt) = stmt else {
                continue;
            };

            let Some(fk_table) = alter_stmt.relation() else {
                continue;
            };

            for cmd in alter_stmt.cmds() {
                let Node::AlterTableCmd(cmd) = cmd else {
                    continue;
                };

                if cmd.subtype != nodes::AlterTableType::AT_AddConstraint {
                    continue;
                }

                let Node::Constraint(cons) = cmd.def() else {
                    continue;
                };

                if cons.contype != nodes::ConstrType::CONSTR_FOREIGN {
                    continue;
                }

                let Some(pk_table) = cons.pktable() else {
                    continue;
                };

                let pk_schema = schema_name(pk_table);
                let pk_table_name = pk_table.relname().unwrap_or_default();
                let fk_schema = schema_name(fk_table);
                let fk_table_name = fk_table.relname().unwrap_or_default();

                for (pk_attr, fk_attr) in cons.pk_attrs().iter().zip(cons.fk_attrs()) {
                    let (Some(pk_col), Some(fk_col)) = (pk_attr.as_str(), fk_attr.as_str()) else {
                        continue;
                    };

                    let pk_column = Column {
                        name: pk_col,
                        table: Some(pk_table_name),
                        schema: Some(pk_schema),
                    };

                    if integer_pks.contains(&pk_column) {
                        result.insert(Column {
                            name: fk_col,
                            table: Some(fk_table_name),
                            schema: Some(fk_schema),
                        });
                    }
                }
            }
        }

        result
    }

    /// Get partitioned parent tables (tables with PARTITION BY).
    fn partitioned_tables(&self) -> HashSet<Table<'_>> {
        let mut result = HashSet::new();

        for stmt in self.stmts.stmts() {
            let Node::CreateStmt(create_stmt) = stmt else {
                continue;
            };

            // Tables with partspec are partitioned parent tables
            if create_stmt.partspec().is_some()
                && let Some(relation) = create_stmt.relation()
            {
                result.insert(Table::from(relation));
            }
        }

        result
    }

    /// Get parent-child relationships from ATTACH PARTITION statements.
    /// Returns a map from child table to parent table.
    fn partition_parents(&self) -> HashMap<Table<'_>, Table<'_>> {
        let mut result = HashMap::new();

        for stmt in self.stmts.stmts() {
            let Node::AlterTableStmt(alter_stmt) = stmt else {
                continue;
            };

            let Some(parent_relation) = alter_stmt.relation() else {
                continue;
            };

            for cmd in alter_stmt.cmds() {
                let Node::AlterTableCmd(cmd) = cmd else {
                    continue;
                };

                if cmd.subtype != nodes::AlterTableType::AT_AttachPartition {
                    continue;
                }

                let Node::PartitionCmd(partition_cmd) = cmd.def() else {
                    continue;
                };

                let Some(child_relation) = partition_cmd.name() else {
                    continue;
                };

                let parent = Table::from(parent_relation);
                let child = Table::from(child_relation);
                result.insert(child, parent);
            }
        }

        result
    }

    /// Get column types for partitioned parent tables after bigint conversion.
    /// Returns a map from (parent_table, column_name) to the converted type.
    fn partitioned_parent_column_types(
        &self,
        columns_to_convert: &HashSet<Column<'_>>,
    ) -> HashMap<(Table<'_>, &str), &'static str> {
        let mut result = HashMap::new();
        let partitioned = self.partitioned_tables();

        for stmt in self.stmts.stmts() {
            let Node::CreateStmt(create_stmt) = stmt else {
                continue;
            };

            let Some(relation) = create_stmt.relation() else {
                continue;
            };

            let table = Table::from(relation);
            if !partitioned.contains(&table) {
                continue;
            }

            let schema = table.schema().map(|s| s.name).unwrap_or("public");
            let table_name = table.name;

            for elt in create_stmt.table_elts() {
                let Node::ColumnDef(col_def) = elt else {
                    continue;
                };

                let col = Column {
                    name: col_def.colname().unwrap_or_default(),
                    table: Some(table_name),
                    schema: Some(schema),
                };

                // Check if this column needs conversion
                if columns_to_convert.contains(&col) {
                    result.insert((table, col_def.colname().unwrap_or_default()), "int8");
                } else if let Some(type_name) = col_def.type_name()
                    && let Some(last_name) = type_name.names().iter().next_back()
                {
                    // Store original type for non-converted columns
                    let type_str = match last_name.sval() {
                        Some("int4") => "int4",
                        Some("int8") => "int8",
                        _ => continue, // Only track integer types
                    };
                    result.insert((table, col_def.colname().unwrap_or_default()), type_str);
                }
            }
        }

        result
    }

    /// Get all column types from CREATE TABLE statements.
    fn column_types(&self) -> HashMap<ColumnTypeKey<'_>, &str> {
        let mut result = HashMap::new();

        for stmt in self.stmts.stmts() {
            let Node::CreateStmt(create_stmt) = stmt else {
                continue;
            };

            let Some(relation) = create_stmt.relation() else {
                continue;
            };

            let schema = schema_name(relation);
            let table_name = relation.relname().unwrap_or_default();

            for elt in create_stmt.table_elts() {
                let Node::ColumnDef(col_def) = elt else {
                    continue;
                };

                if let Some(type_name) = col_def.type_name()
                    && let Some(last_name) = type_name.names().iter().next_back()
                {
                    result.insert(
                        ColumnTypeKey {
                            schema,
                            table: table_name,
                            column: col_def.colname().unwrap_or_default(),
                        },
                        last_name.sval().unwrap_or_default(),
                    );
                }
            }
        }

        result
    }

    /// Rewrite the schema and return the slice of it belonging to `state`.
    pub(crate) fn statements(&self, state: SyncState) -> Result<Vec<Statement>, SchemaSyncError> {
        let mut result = vec![];

        // Get integer PK and FK columns that need bigint conversion
        let columns_to_convert: HashSet<Column<'_>> = self
            .integer_primary_key_columns()
            .union(&self.integer_foreign_key_columns())
            .copied()
            .collect();

        // Get partitioned parent column types and parent-child relationships
        let parent_column_types = self.partitioned_parent_column_types(&columns_to_convert);
        let partition_parents = self.partition_parents();

        for stmt in self.stmts.into_iter() {
            let original = self
                .original
                .get(stmt.stmt_location as usize..(stmt.stmt_location + stmt.stmt_len) as usize)
                .ok_or(SchemaSyncError::StmtOutOfBounds)?;

            match stmt.stmt() {
                Node::CreateStmt(create_stmt) => {
                    let table = create_stmt.relation().map(Table::from).unwrap_or_default();

                    let stmt = make::owned(|mem| {
                        let mut stmt = mem.make_unique(create_stmt);
                        stmt.as_mut().set_if_not_exists(true);

                        // Get table info
                        let schema = table.schema().map(|s| s.name).unwrap_or("public");
                        let table_name = table.name;

                        // Check if this table is a child partition
                        let parent_table = partition_parents.get(&table);

                        // Convert integer PK/FK columns to bigint
                        for elt in stmt.as_mut().table_elts_mut() {
                            let NodeMut::ColumnDef(mut col_def) = elt else {
                                continue;
                            };

                            let col = Column {
                                name: col_def.colname().unwrap_or_default(),
                                table: Some(table_name),
                                schema: Some(schema),
                            };

                            if should_convert_to_bigint(
                                &col,
                                col_def.type_name(),
                                &columns_to_convert,
                                parent_table,
                                &parent_column_types,
                            ) && let Some(mut type_name) = col_def.type_name_mut()
                            {
                                type_name.set_names(mem.make_list(&[
                                    mem.make_string(Some("pg_catalog")),
                                    mem.make_string(Some("int8")),
                                ]));
                            }
                        }
                        stmt
                    });

                    if state == SyncState::PreData {
                        result.push(deparsed(&*stmt)?);
                    }
                }

                Node::CreateSeqStmt(stmt) => {
                    let stmt = make::owned(|mem| {
                        let mut stmt = mem.make_unique(&*stmt);
                        stmt.as_mut().set_if_not_exists(true);
                        stmt
                    });
                    if state == SyncState::PreData {
                        result.push(deparsed(&*stmt)?);
                    }
                }

                Node::CreateExtensionStmt(stmt) => {
                    let stmt = make::owned(|mem| {
                        let mut stmt = mem.make_unique(&*stmt);
                        stmt.as_mut().set_if_not_exists(true);
                        stmt
                    });
                    if state == SyncState::PreData {
                        result.push(deparsed(&*stmt)?);
                    }
                }

                Node::CreateSchemaStmt(stmt) => {
                    let stmt = make::owned(|mem| {
                        let mut stmt = mem.make_unique(&*stmt);
                        stmt.as_mut().set_if_not_exists(true);
                        stmt
                    });
                    if state == SyncState::PreData {
                        result.push(deparsed(&*stmt)?);
                    }
                }

                Node::AlterTableStmt(stmt) => {
                    for cmd in stmt.cmds() {
                        if let Node::AlterTableCmd(cmd) = cmd {
                            match cmd.subtype {
                                nodes::AlterTableType::AT_AddConstraint => {
                                    if let Node::Constraint(cons) = cmd.def() {
                                        // Only allow primary key constraints.
                                        if matches!(
                                            cons.contype,
                                            nodes::ConstrType::CONSTR_PRIMARY
                                                | nodes::ConstrType::CONSTR_NOTNULL
                                                | nodes::ConstrType::CONSTR_NULL
                                        ) {
                                            // Integer PKs are already tracked and converted
                                            // to bigint in CreateStmt handler
                                            if state == SyncState::PreData {
                                                result.push(
                                                    Statement::new(original).set_skip_if_exists(),
                                                );
                                            }
                                        } else if cons.contype == nodes::ConstrType::CONSTR_FOREIGN
                                        {
                                            // FK columns referencing integer PKs are
                                            // computed from fk_columns at the end
                                            if state == SyncState::PostData {
                                                result.push(
                                                    Statement::new(original).set_skip_if_exists(),
                                                );
                                            }
                                        } else if state == SyncState::PostData {
                                            result.push(
                                                Statement::new(original).set_skip_if_exists(),
                                            );
                                        }
                                    }
                                }
                                nodes::AlterTableType::AT_AttachPartition => {
                                    match stmt.objtype {
                                        // Index partitions need to be attached to indexes,
                                        // which we create in the post-data step.
                                        nodes::ObjectType::OBJECT_INDEX => {
                                            if state == SyncState::PostData {
                                                result.push(
                                                    Statement::new(original).set_skip_if_exists(),
                                                );
                                            }
                                        }

                                        // Table partitions are attached in pre-data
                                        // after the partition tables are created.
                                        nodes::ObjectType::OBJECT_TABLE => {
                                            if state == SyncState::PreData {
                                                result.push(
                                                    Statement::new(original).set_skip_if_exists(),
                                                );
                                            }
                                        }

                                        _ => {
                                            if state == SyncState::PreData {
                                                result.push(
                                                    Statement::new(original).set_skip_if_exists(),
                                                );
                                            }
                                        }
                                    }
                                }

                                nodes::AlterTableType::AT_ColumnDefault => {
                                    if state == SyncState::PreData {
                                        result.push(Statement::new(original));
                                    }
                                }

                                nodes::AlterTableType::AT_AddIdentity => (),

                                // REPLICA IDENTITY USING INDEX references an
                                // index, which is created in the post-data
                                // step. In pre-data it fails, and a restore
                                // that ignores errors silently leaves the
                                // table with its default identity. FULL,
                                // NOTHING and DEFAULT ride along: the table
                                // exists either way.
                                nodes::AlterTableType::AT_ReplicaIdentity => {
                                    if state == SyncState::PostData {
                                        result.push(Statement::new(original));
                                    }
                                }

                                // AlterTableType::AtChangeOwner => {
                                //     continue; // Don't change owners, for now.
                                // }
                                _ => {
                                    if state == SyncState::PreData {
                                        result.push(Statement::new(original));
                                    }
                                }
                            }
                        }
                    }
                }

                Node::CreateTrigStmt(stmt) => {
                    let stmt = make::owned(|mem| {
                        let mut stmt = mem.make_unique(&*stmt);
                        stmt.as_mut().set_replace(true);
                        stmt
                    });

                    if state == SyncState::PreData {
                        result.push(deparsed(&*stmt)?);
                    }
                }

                Node::CreatePublicationStmt(stmt) => {
                    if state == SyncState::PreData {
                        // DROP first for idempotency
                        result.push(Statement::new(format!(
                            "DROP PUBLICATION IF EXISTS \"{}\"",
                            crate::util::escape_identifier(stmt.pubname().unwrap_or_default())
                        )));
                        result.push(Statement::new(original).set_skip_if_exists());
                    }
                }

                Node::AlterPublicationStmt(_) => {
                    if state == SyncState::PreData {
                        result.push(Statement::new(original).set_skip_if_exists());
                    }
                }

                // Skip these.
                Node::CreateSubscriptionStmt(_) | Node::AlterSubscriptionStmt(_) => (),

                Node::AlterSeqStmt(stmt) => {
                    if matches!(state, SyncState::PreData | SyncState::Cutover) {
                        let sequence = stmt
                            .sequence()
                            .map(Table::from)
                            .ok_or(SchemaSyncError::MissingEntity)?;
                        let sequence = Sequence::from(sequence);
                        let column = stmt
                            .options()
                            .first()
                            .ok_or(SchemaSyncError::MissingEntity)?;
                        let column =
                            Column::try_from(column).map_err(|_| SchemaSyncError::MissingEntity)?;

                        if state == SyncState::PreData {
                            result.push(Statement::new(original));
                        } else if state == SyncState::Cutover {
                            let sql = sequence
                                .setval_from_column(&column)
                                .map_err(|_| SchemaSyncError::MissingEntity)?;
                            result.push(Statement::new(sql))
                        }
                    }
                }

                Node::IndexStmt(stmt) => {
                    if state == SyncState::PostData {
                        let changed_stmt = make::owned(|mem| {
                            let mut stmt = mem.make_unique(&*stmt);
                            let concurrent = stmt.relation().is_some_and(|relation| relation.inh);
                            stmt.as_mut().set_concurrent(concurrent); // ONLY used for partitioned tables, which can't be created concurrently.
                            stmt.as_mut().set_if_not_exists(true);
                            stmt
                        });

                        let index_schema = stmt.relation().map(schema_name).unwrap_or("public");
                        result.push(Statement::new(format!(
                            "DROP INDEX IF EXISTS \"{}\".\"{}\"",
                            index_schema,
                            stmt.idxname().expect("name always present"),
                        )));

                        result.push(deparsed(&*changed_stmt)?);
                    }
                }

                Node::ViewStmt(stmt) => {
                    let stmt = make::owned(|mem| {
                        let mut stmt = mem.make_unique(&*stmt);
                        stmt.as_mut().set_replace(true);
                        stmt
                    });

                    if state == SyncState::PreData {
                        result.push(deparsed(&*stmt)?);
                    }
                }

                Node::CreateTableAsStmt(stmt) => {
                    let stmt = make::owned(|mem| {
                        let mut stmt = mem.make_unique(&*stmt);
                        stmt.as_mut().set_if_not_exists(true);
                        stmt
                    });

                    if state == SyncState::PreData {
                        result.push(deparsed(&*stmt)?);
                    }
                }

                Node::CreateFunctionStmt(stmt) => {
                    let stmt = make::owned(|mem| {
                        let mut stmt = mem.make_unique(&*stmt);
                        stmt.as_mut().set_replace(true);
                        stmt
                    });

                    if state == SyncState::PreData {
                        result.push(deparsed(&*stmt)?);
                    }
                }

                Node::AlterOwnerStmt(stmt) => {
                    if stmt.object_type != nodes::ObjectType::OBJECT_PUBLICATION
                        && state == SyncState::PreData
                    {
                        result.push(Statement::new(original));
                    }
                }

                Node::CreateEnumStmt(_)
                | Node::CreateDomainStmt(_)
                | Node::CompositeTypeStmt(_) => {
                    if state == SyncState::PreData {
                        result.push(Statement::new(original).set_skip_if_exists());
                    }
                }

                Node::VariableSetStmt(_) => continue,
                Node::SelectStmt(_) => continue,
                _ => {
                    if state == SyncState::PreData {
                        result.push(Statement::new(original));
                    }
                }
            }
        }

        Ok(result)
    }
}

#[cfg(test)]
mod test {
    use std::ffi::OsStr;

    use crate::config::ServerAuth;

    use super::*;

    #[tokio::test]
    async fn test_pg_dump_execute() {
        let cluster = Cluster::new_test_single_shard(&config());
        let _pg_dump = PgDump::new(&cluster, "test_pg_dump_execute");
    }

    #[test]
    fn test_build_pg_dump_command_sets_password_env() {
        let addr = backend::pool::Address::new_test();
        let command = build_pg_dump_command("pg_dump", &addr, "secret");

        let env = command
            .as_std()
            .get_envs()
            .find(|(key, _)| *key == OsStr::new("PGPASSWORD"))
            .and_then(|(_, value)| value);

        assert_eq!(env, Some(OsStr::new("secret")));

        let sslmode = command
            .as_std()
            .get_envs()
            .find(|(key, _)| *key == OsStr::new("PGSSLMODE"))
            .and_then(|(_, value)| value);

        assert_eq!(sslmode, None);
    }

    #[test]
    fn test_build_pg_dump_command_sets_tls_for_rds_iam() {
        let mut addr = backend::pool::Address::new_test();
        addr.server_auth = ServerAuth::RdsIam;
        let command = build_pg_dump_command("pg_dump", &addr, "token");

        let sslmode = command
            .as_std()
            .get_envs()
            .find(|(key, _)| *key == OsStr::new("PGSSLMODE"))
            .and_then(|(_, value)| value);

        assert_eq!(sslmode, Some(OsStr::new("require")));
    }

    #[test]
    fn test_build_pg_dump_command_sets_tls_for_azure_workload_identity() {
        let mut addr = backend::pool::Address::new_test();
        addr.server_auth = ServerAuth::AzureWorkloadIdentity;
        let command = build_pg_dump_command("pg_dump", &addr, "token");

        let sslmode = command
            .as_std()
            .get_envs()
            .find(|(key, _)| *key == OsStr::new("PGSSLMODE"))
            .and_then(|(_, value)| value);

        assert_eq!(sslmode, Some(OsStr::new("require")));
    }

    #[test]
    fn test_specific_dump() {
        let dump = r#"
-- PostgreSQL database dump
--

\restrict nu6jB5ogH2xGMn2dB3dMyMbSZ2PsVDqB2IaWK6zZVjngeba0UrnmxMy6s63SwzR

-- Dumped from database version 16.6
-- Dumped by pg_dump version 16.10 (Ubuntu 16.10-0ubuntu0.24.04.1)

SET statement_timeout = 0;
SET lock_timeout = 0;
SET idle_in_transaction_session_timeout = 0;
SET client_encoding = 'UTF8';
SET standard_conforming_strings = on;
SELECT pg_catalog.set_config('search_path', '', false);
SET check_function_bodies = false;
SET xmloption = content;
SET client_min_messages = warning;
SET row_security = off;

SET default_tablespace = '';

SET default_table_access_method = heap;

--
-- Name: users; Type: TABLE; Schema: public; Owner: pgdog-4
--

CREATE TABLE public.users (
    id bigint NOT NULL,
    email character varying NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL
);


ALTER TABLE public.users OWNER TO "pgdog-4";

--
-- Name: users users_pkey; Type: CONSTRAINT; Schema: public; Owner: pgdog-4
--

ALTER TABLE ONLY public.users
    ADD CONSTRAINT users_pkey PRIMARY KEY (id);


--
-- PostgreSQL database dump complete
--

\unrestrict nu6jB5ogH2xGMn2dB3dMyMbSZ2PsVDqB2IaWK6zZVjngeba0UrnmxMy6s63SwzR
"#;
        let _parse = pg_raw_parse::parse(&PgDump::clean(dump)).unwrap();
    }

    #[test]
    fn test_generated_identity() {
        let output = parse(
            "ALTER TABLE public.users ALTER COLUMN id ADD GENERATED ALWAYS AS IDENTITY (
            SEQUENCE NAME public.users_id_seq
            START WITH 1
            INCREMENT BY 1
            NO MINVALUE
            NO MAXVALUE
            CACHE 1
        );",
        );

        // Identity constraints should be skipped in PreData
        let statements = output.statements(SyncState::PreData).unwrap();
        assert!(statements.is_empty());

        // Identity constraints should be skipped in Cutover
        let statements = output.statements(SyncState::Cutover).unwrap();
        assert!(statements.is_empty());

        let statements = output.statements(SyncState::PostData).unwrap();
        assert!(statements.is_empty());
    }

    #[test]
    fn test_integer_primary_key_columns() {
        let output = parse(
            r#"
CREATE TABLE users (id INTEGER, name TEXT);
ALTER TABLE users ADD CONSTRAINT users_pkey PRIMARY KEY (id);"#,
        );

        let pk_columns = output.integer_primary_key_columns();

        // Should have one integer primary key column
        assert_eq!(pk_columns.len(), 1);
        assert!(pk_columns.contains(&Column {
            name: "id",
            table: Some("users"),
            schema: Some("public"),
        }));
    }

    #[test]
    fn test_non_integer_pk_excluded() {
        let output = parse(
            r#"
CREATE TABLE users (id UUID, name TEXT);
ALTER TABLE users ADD CONSTRAINT users_pkey PRIMARY KEY (id);"#,
        );

        let pk_columns = output.integer_primary_key_columns();

        // UUID primary key should not be included
        assert_eq!(pk_columns.len(), 0);
    }

    #[test]
    fn test_integer_foreign_key_columns() {
        let output = parse(
            r#"
CREATE TABLE parent (id INTEGER, name TEXT);
CREATE TABLE child (id INTEGER, parent_id INTEGER);
ALTER TABLE parent ADD CONSTRAINT parent_pkey PRIMARY KEY (id);
ALTER TABLE child ADD CONSTRAINT child_parent_fk FOREIGN KEY (parent_id) REFERENCES parent(id);"#,
        );

        let fk_columns = output.integer_foreign_key_columns();

        // Should have one integer FK column
        assert_eq!(fk_columns.len(), 1);
        assert!(fk_columns.contains(&Column {
            name: "parent_id",
            table: Some("child"),
            schema: Some("public"),
        }));
    }

    #[test]
    fn test_integer_foreign_key_columns_composite() {
        let output = parse(
            r#"
CREATE TABLE parent (id1 INTEGER, id2 INTEGER, name TEXT);
CREATE TABLE child (id INTEGER, parent_id1 INTEGER, parent_id2 INTEGER);
ALTER TABLE parent ADD CONSTRAINT parent_pkey PRIMARY KEY (id1, id2);
ALTER TABLE child ADD CONSTRAINT child_parent_fk FOREIGN KEY (parent_id1, parent_id2) REFERENCES parent(id1, id2);"#,
        );

        let fk_columns = output.integer_foreign_key_columns();

        // Should have two integer FK columns
        assert_eq!(fk_columns.len(), 2);
        assert!(fk_columns.contains(&Column {
            name: "parent_id1",
            table: Some("child"),
            schema: Some("public"),
        }));
        assert!(fk_columns.contains(&Column {
            name: "parent_id2",
            table: Some("child"),
            schema: Some("public"),
        }));
    }

    #[test]
    fn test_integer_primary_key_columns_composite() {
        let output = parse(
            r#"
CREATE TABLE order_items (order_id INTEGER, item_id INTEGER, quantity INTEGER);
ALTER TABLE order_items ADD CONSTRAINT order_items_pkey PRIMARY KEY (order_id, item_id);"#,
        );

        let pk_columns = output.integer_primary_key_columns();

        // Should have two integer primary key columns
        assert_eq!(pk_columns.len(), 2);
        assert!(pk_columns.contains(&Column {
            name: "order_id",
            table: Some("order_items"),
            schema: Some("public"),
        }));
        assert!(pk_columns.contains(&Column {
            name: "item_id",
            table: Some("order_items"),
            schema: Some("public"),
        }));
    }

    #[test]
    fn test_bigint_rewrite() {
        let output = parse(
            r#"
CREATE TABLE test (id INTEGER, value TEXT);
ALTER TABLE test ADD CONSTRAINT id_pkey PRIMARY KEY (id);"#,
        );

        let statements = output.statements(SyncState::PreData).unwrap();
        assert_eq!(statements.len(), 2);

        // Integer PK column should be converted to bigint directly in CREATE TABLE
        assert_eq!(
            statements[0].sql,
            "CREATE TABLE IF NOT EXISTS test (id bigint, value text)"
        );
        assert_eq!(
            statements[1].sql,
            "ALTER TABLE test ADD CONSTRAINT id_pkey PRIMARY KEY (id)"
        );
    }

    #[test]
    fn test_bigint_rewrite_foreign_key() {
        let output = parse(
            r#"
CREATE TABLE parent (id INTEGER, name TEXT);
CREATE TABLE child (id INTEGER, parent_id INTEGER);
ALTER TABLE parent ADD CONSTRAINT parent_pkey PRIMARY KEY (id);
ALTER TABLE child ADD CONSTRAINT child_parent_fk FOREIGN KEY (parent_id) REFERENCES parent(id);"#,
        );

        let statements = output.statements(SyncState::PreData).unwrap();
        assert_eq!(statements.len(), 3);

        // PK column converted to bigint in CREATE TABLE
        assert_eq!(
            statements[0].sql,
            "CREATE TABLE IF NOT EXISTS parent (id bigint, name text)"
        );
        // FK column also converted to bigint in CREATE TABLE
        assert_eq!(
            statements[1].sql,
            "CREATE TABLE IF NOT EXISTS child (id int, parent_id bigint)"
        );
        assert_eq!(
            statements[2].sql,
            "ALTER TABLE parent ADD CONSTRAINT parent_pkey PRIMARY KEY (id)"
        );
    }

    #[test]
    fn test_attach_partition() {
        // pg_dump generates ATTACH PARTITION for partitioned tables
        let output = parse(
            r#"
CREATE TABLE parent (id INTEGER, created_at DATE) PARTITION BY RANGE (created_at);
CREATE TABLE parent_2024 (id INTEGER, created_at DATE);
ALTER TABLE ONLY parent ATTACH PARTITION parent_2024 FOR VALUES FROM ('2024-01-01') TO ('2025-01-01');"#,
        );

        let pre_data = output.statements(SyncState::PreData).unwrap();
        let post_data = output.statements(SyncState::PostData).unwrap();

        // CREATE TABLEs should be in pre-data
        assert_eq!(pre_data.len(), 3);

        // ATTACH PARTITION for tables should be in pre-data, not post-data
        assert!(pre_data[2].sql.contains("ATTACH PARTITION"));

        // No statements in post-data for table partitions
        assert!(post_data.is_empty());
    }

    #[test]
    fn test_replica_identity_runs_after_indexes() {
        // pg_dump emits REPLICA IDENTITY USING INDEX right after the
        // index it references, which is created in the post-data step.
        // Running it in pre-data fails, and a restore that ignores
        // errors silently leaves the table with its default identity.
        let output = parse(
            r#"
CREATE TABLE commands (id BIGINT NOT NULL, organization_id BIGINT NOT NULL);
CREATE UNIQUE INDEX commands_org_idx ON commands (id, organization_id);
ALTER TABLE ONLY commands REPLICA IDENTITY USING INDEX commands_org_idx;
CREATE TABLE events (id BIGINT NOT NULL);
ALTER TABLE ONLY events REPLICA IDENTITY FULL;"#,
        );

        let pre_data = output.statements(SyncState::PreData).unwrap();
        let post_data = output.statements(SyncState::PostData).unwrap();

        assert!(
            pre_data
                .iter()
                .all(|stmt| !stmt.sql.contains("REPLICA IDENTITY")),
            "replica identity must not run before its index exists"
        );

        let identities: Vec<_> = post_data
            .iter()
            .filter(|stmt| stmt.sql.contains("REPLICA IDENTITY"))
            .collect();
        assert_eq!(identities.len(), 2);
        assert!(
            identities[0]
                .sql
                .contains("REPLICA IDENTITY USING INDEX commands_org_idx")
        );
        assert!(identities[1].sql.contains("REPLICA IDENTITY FULL"));

        // The index it references is created earlier in the same step.
        let index_position = post_data
            .iter()
            .position(|stmt| stmt.sql.contains("CREATE UNIQUE INDEX"))
            .expect("the index restores in post-data");
        let identity_position = post_data
            .iter()
            .position(|stmt| stmt.sql.contains("REPLICA IDENTITY USING INDEX"))
            .expect("the identity restores in post-data");
        assert!(index_position < identity_position);
    }

    #[test]
    fn test_create_publication_restored() {
        let output = parse("CREATE PUBLICATION my_pub FOR TABLE users, orders;");

        let statements = output.statements(SyncState::PreData).unwrap();

        // Should have DROP and CREATE statements
        assert_eq!(statements.len(), 2);
        assert_eq!(statements[0].sql, "DROP PUBLICATION IF EXISTS \"my_pub\"");
        assert_eq!(
            statements[1].sql,
            "CREATE PUBLICATION my_pub FOR TABLE users, orders"
        );
    }

    #[test]
    fn test_alter_publication_add_table_restored() {
        // pg_dump outputs publication tables as ALTER PUBLICATION ... ADD TABLE
        let output = parse("ALTER PUBLICATION my_pub ADD TABLE ONLY public.users;");

        let statements = output.statements(SyncState::PreData).unwrap();

        assert_eq!(statements.len(), 1);
        assert!(
            statements[0]
                .sql
                .contains("ALTER PUBLICATION my_pub ADD TABLE")
        );
    }

    #[test]
    fn test_partitioned_child_inherits_bigint_from_parent() {
        // pg_dump generates FK constraints only for parent tables, not child partitions.
        // When parent has integer columns converted to bigint (via PK/FK),
        // child partitions should also have those columns converted.
        let output = parse(
            r#"
CREATE TABLE users (id INTEGER);
CREATE TABLE orders (id INTEGER, user_id INTEGER, created_at DATE) PARTITION BY RANGE (created_at);
CREATE TABLE orders_2024 (id INTEGER, user_id INTEGER, created_at DATE);
ALTER TABLE users ADD CONSTRAINT users_pkey PRIMARY KEY (id);
ALTER TABLE orders ADD CONSTRAINT orders_pkey PRIMARY KEY (id);
ALTER TABLE orders ADD CONSTRAINT orders_user_fk FOREIGN KEY (user_id) REFERENCES users(id);
ALTER TABLE ONLY orders ATTACH PARTITION orders_2024 FOR VALUES FROM ('2024-01-01') TO ('2025-01-01');"#,
        );

        let statements = output.statements(SyncState::PreData).unwrap();

        // Find the parent table statement
        let parent_sql = statements
            .iter()
            .map(|stmt| stmt.sql.as_str())
            .find(|sql| sql.contains("orders") && !sql.contains("orders_2024"))
            .expect("should find parent table");
        assert!(
            parent_sql.contains("id bigint"),
            "parent id should be bigint: {}",
            parent_sql
        );
        assert!(
            parent_sql.contains("user_id bigint"),
            "parent user_id should be bigint: {}",
            parent_sql
        );

        // Find the child table statement
        let child_sql = statements
            .iter()
            .map(|stmt| stmt.sql.as_str())
            .find(|sql| sql.contains("orders_2024"))
            .expect("should find child table");
        assert!(
            child_sql.contains("id bigint"),
            "child id should be bigint: {}",
            child_sql
        );
        assert!(
            child_sql.contains("user_id bigint"),
            "child user_id should be bigint: {}",
            child_sql
        );
    }

    fn collapsed(sql: &str) -> String {
        Collapsed(sql).to_string()
    }

    #[test]
    fn test_collapsed_joins_lines_with_one_space() {
        assert_eq!(
            collapsed(
                r#"CREATE TABLE test
    (id bigint,

     name text)"#
            ),
            "CREATE TABLE test (id bigint, name text)"
        );
    }

    #[test]
    fn test_collapsed_drops_comments_and_blank_lines() {
        assert_eq!(
            collapsed(
                r#"-- a comment
SELECT 1

   -- another
FROM t"#
            ),
            "SELECT 1 FROM t"
        );
    }

    #[test]
    fn test_collapsed_has_no_space_inside_brackets() {
        assert_eq!(
            collapsed(
                r#"CREATE TYPE geo AS (
    lat numeric(9,6),
    lon numeric(9,6)
)"#
            ),
            "CREATE TYPE geo AS (lat numeric(9,6), lon numeric(9,6))"
        );
        assert_eq!(
            collapsed(
                r#"SELECT tags[
    1
]"#
            ),
            "SELECT tags[1]"
        );
    }

    #[test]
    fn test_collapsed_has_no_space_before_comma_or_semicolon() {
        assert_eq!(
            collapsed(
                r#"SELECT a
,
b
;"#
            ),
            "SELECT a, b;"
        );
    }

    #[test]
    fn test_collapsed_trims_edges() {
        assert_eq!(
            collapsed(
                r#"
   SELECT 1

"#
            ),
            "SELECT 1"
        );
        assert_eq!(
            collapsed(
                r#"

"#
            ),
            ""
        );
    }

    #[test]
    fn test_collapsed_treats_tabs_as_spaces() {
        assert_eq!(collapsed("\tSELECT\t\t1\t"), "SELECT 1");
    }

    #[test]
    fn test_collapsed_truncates_long_sql() {
        let long = format!("SELECT {}", "a".repeat(500));
        let collapsed = collapsed(&long);

        assert!(collapsed.starts_with("SELECT aaa"));
        assert!(collapsed.ends_with('…'));
        assert_eq!(collapsed.chars().count(), LOG_CHARS + 1);
    }

    #[test]
    fn test_collapsed_keeps_sql_at_the_limit_whole() {
        let exact = "a".repeat(LOG_CHARS);

        assert_eq!(collapsed(&exact), exact);
    }

    fn parse(query: &str) -> PgDumpOutput {
        PgDumpOutput {
            stmts: pg_raw_parse::parse(query).unwrap().into_inner(),
            original: query.to_owned(),
        }
    }
}
