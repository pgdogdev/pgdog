//! Wrapper around pg_dump.

use std::{
    collections::{HashMap, HashSet},
    ops::Deref,
    str::from_utf8,
};

use lazy_static::lazy_static;
use pg_query::{
    protobuf::{AlterTableType, ConstrType, ObjectType, ParseResult, RangeVar, String as PgString},
    Node, NodeEnum,
};
use pgdog_config::QueryParserEngine;
use regex::Regex;
use tracing::{info, trace, warn};

use super::{progress::Progress, Error};
use crate::{
    backend::{self, pool::Request, replication::publisher::PublicationTable, Cluster},
    config::config,
    frontend::router::parser::{sequence::Sequence, Column, Table},
};

/// Key for looking up column types during pg_dump parsing.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
struct ColumnTypeKey<'a> {
    schema: &'a str,
    table: &'a str,
    column: &'a str,
}

fn deparse_node(node: NodeEnum) -> Result<String, pg_query::Error> {
    match config().config.general.query_parser_engine {
        QueryParserEngine::PgQueryProtobuf => node.deparse(),
        QueryParserEngine::PgQueryRaw => node.deparse_raw(),
    }
}

fn parse(query: &str) -> Result<pg_query::ParseResult, pg_query::Error> {
    match config().config.general.query_parser_engine {
        QueryParserEngine::PgQueryProtobuf => pg_query::parse(query),
        QueryParserEngine::PgQueryRaw => pg_query::parse_raw(query),
    }
}

fn schema_name(relation: &RangeVar) -> &str {
    if relation.schemaname.is_empty() {
        "public"
    } else {
        relation.schemaname.as_str()
    }
}

use tokio::process::Command;

#[derive(Debug, Clone)]
pub struct PgDump {
    source: Cluster,
    publication: String,
}

impl PgDump {
    pub fn new(source: &Cluster, publication: &str) -> Self {
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

    /// Dump schema from source cluster.
    pub async fn dump(&self) -> Result<PgDumpOutput, Error> {
        let mut comparison: Vec<PublicationTable> = vec![];
        let addr = self
            .source
            .shards()
            .first()
            .ok_or(Error::NoDatabases)?
            .pools()
            .first()
            .ok_or(Error::NoDatabases)?
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
            return Err(Error::PublicationNoTables(self.publication.clone()));
        }

        info!("dumping schema [{}, {}]", comparison.len(), addr,);

        let config = config();
        let pg_dump_path = config
            .config
            .replication
            .pg_dump_path
            .to_str()
            .unwrap_or("pg_dump");

        let output = Command::new(pg_dump_path)
            .arg("--schema-only")
            .arg("-h")
            .arg(&addr.host)
            .arg("-p")
            .arg(addr.port.to_string())
            .arg("-U")
            .arg(&addr.user)
            .env("PGPASSWORD", &addr.password)
            .arg("-d")
            .arg(&addr.database_name)
            .output()
            .await?;

        if !output.status.success() {
            let err = from_utf8(&output.stderr)?;
            return Err(Error::PgDump(err.to_string()));
        }

        let original = from_utf8(&output.stdout)?.to_string();
        trace!("[pg_dump (original)] {}", original);

        let cleaned = Self::clean(&original);
        trace!("[pg_dump (clean)] {}", cleaned);

        let stmts = parse(&cleaned)?.protobuf;

        Ok(PgDumpOutput {
            stmts,
            original: cleaned,
        })
    }
}

#[derive(Debug, Clone)]
pub struct PgDumpOutput {
    stmts: ParseResult,
    original: String,
}

#[derive(Debug, Copy, Clone, PartialEq)]
pub enum SyncState {
    PreData,
    PostData,
    Cutover,
}

#[derive(Debug)]
pub enum Statement<'a> {
    Index {
        table: Table<'a>,
        name: &'a str,
        sql: String,
    },

    Table {
        table: Table<'a>,
        sql: String,
    },

    Other {
        sql: String,
        idempotent: bool,
    },

    SequenceOwner {
        column: Column<'a>,
        sequence: Sequence<'a>,
        sql: &'a str,
    },

    SequenceSetMax {
        sequence: Sequence<'a>,
        sql: String,
    },
}

impl<'a> Deref for Statement<'a> {
    type Target = str;
    fn deref(&self) -> &Self::Target {
        match self {
            Self::Index { sql, .. } => sql,
            Self::Table { sql, .. } => sql,
            Self::SequenceOwner { sql, .. } => sql,
            Self::Other { sql, .. } => sql,
            Self::SequenceSetMax { sql, .. } => sql.as_str(),
        }
    }
}

impl<'a> From<&'a str> for Statement<'a> {
    fn from(value: &'a str) -> Self {
        Self::Other {
            sql: value.to_string(),
            idempotent: true,
        }
    }
}

impl<'a> From<String> for Statement<'a> {
    fn from(value: String) -> Self {
        Self::Other {
            sql: value,
            idempotent: true,
        }
    }
}

impl PgDumpOutput {
    /// Get integer primary key columns (columns that are part of PRIMARY KEY
    /// constraints and have integer types like int4, int2, serial, etc.).
    pub fn integer_primary_key_columns(&self) -> HashSet<Column<'_>> {
        let column_types = self.column_types();
        let mut result = HashSet::new();

        for stmt in &self.stmts.stmts {
            let Some(ref node) = stmt.stmt else {
                continue;
            };
            let Some(NodeEnum::AlterTableStmt(ref alter_stmt)) = node.node else {
                continue;
            };

            let Some(ref relation) = alter_stmt.relation else {
                continue;
            };

            for cmd in &alter_stmt.cmds {
                let Some(NodeEnum::AlterTableCmd(ref cmd)) = cmd.node else {
                    continue;
                };

                if cmd.subtype() != AlterTableType::AtAddConstraint {
                    continue;
                }

                let Some(ref def) = cmd.def else {
                    continue;
                };

                let Some(NodeEnum::Constraint(ref cons)) = def.node else {
                    continue;
                };

                if cons.contype() != ConstrType::ConstrPrimary {
                    continue;
                }

                let schema = schema_name(relation);
                let table_name = relation.relname.as_str();

                for key in &cons.keys {
                    let Some(NodeEnum::String(PgString { sval })) = &key.node else {
                        continue;
                    };

                    let col_name = sval.as_str();
                    let type_key = ColumnTypeKey {
                        schema,
                        table: table_name,
                        column: col_name,
                    };

                    let is_integer = column_types
                        .get(&type_key)
                        .map(|t| {
                            matches!(
                                *t,
                                "int4" | "int2" | "serial" | "smallserial" | "integer" | "smallint"
                            )
                        })
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
    pub fn integer_foreign_key_columns(&self) -> HashSet<Column<'_>> {
        let integer_pks = self.integer_primary_key_columns();
        let mut result = HashSet::new();

        for stmt in &self.stmts.stmts {
            let Some(ref node) = stmt.stmt else {
                continue;
            };
            let Some(NodeEnum::AlterTableStmt(ref alter_stmt)) = node.node else {
                continue;
            };

            let Some(ref fk_table) = alter_stmt.relation else {
                continue;
            };

            for cmd in &alter_stmt.cmds {
                let Some(NodeEnum::AlterTableCmd(ref cmd)) = cmd.node else {
                    continue;
                };

                if cmd.subtype() != AlterTableType::AtAddConstraint {
                    continue;
                }

                let Some(ref def) = cmd.def else {
                    continue;
                };

                let Some(NodeEnum::Constraint(ref cons)) = def.node else {
                    continue;
                };

                if cons.contype() != ConstrType::ConstrForeign {
                    continue;
                }

                let Some(ref pk_table) = cons.pktable else {
                    continue;
                };

                let pk_schema = schema_name(pk_table);
                let pk_table_name = pk_table.relname.as_str();
                let fk_schema = schema_name(fk_table);
                let fk_table_name = fk_table.relname.as_str();

                for (pk_attr, fk_attr) in cons.pk_attrs.iter().zip(cons.fk_attrs.iter()) {
                    let (
                        Some(NodeEnum::String(PgString { sval: pk_col })),
                        Some(NodeEnum::String(PgString { sval: fk_col })),
                    ) = (&pk_attr.node, &fk_attr.node)
                    else {
                        continue;
                    };

                    let pk_column = Column {
                        name: pk_col.as_str(),
                        table: Some(pk_table_name),
                        schema: Some(pk_schema),
                    };

                    if integer_pks.contains(&pk_column) {
                        result.insert(Column {
                            name: fk_col.as_str(),
                            table: Some(fk_table_name),
                            schema: Some(fk_schema),
                        });
                    }
                }
            }
        }

        result
    }

    /// Get all column types from CREATE TABLE statements.
    fn column_types(&self) -> HashMap<ColumnTypeKey<'_>, &str> {
        let mut result = HashMap::new();

        for stmt in &self.stmts.stmts {
            let Some(ref node) = stmt.stmt else {
                continue;
            };
            let Some(NodeEnum::CreateStmt(ref create_stmt)) = node.node else {
                continue;
            };

            let Some(ref relation) = create_stmt.relation else {
                continue;
            };

            let schema = schema_name(relation);
            let table_name = relation.relname.as_str();

            for elt in &create_stmt.table_elts {
                if let Some(NodeEnum::ColumnDef(col_def)) = &elt.node {
                    if let Some(ref type_name) = col_def.type_name {
                        if let Some(last_name) = type_name.names.last() {
                            if let Some(NodeEnum::String(PgString { sval })) = &last_name.node {
                                result.insert(
                                    ColumnTypeKey {
                                        schema,
                                        table: table_name,
                                        column: col_def.colname.as_str(),
                                    },
                                    sval.as_str(),
                                );
                            }
                        }
                    }
                }
            }
        }

        result
    }

    /// Get schema statements to execute before data sync,
    /// e.g., CREATE TABLE, primary key.
    pub fn statements(&self, state: SyncState) -> Result<Vec<Statement<'_>>, Error> {
        let mut result = vec![];

        // Get integer PK and FK columns that need bigint conversion
        let columns_to_convert: HashSet<Column<'_>> = self
            .integer_primary_key_columns()
            .union(&self.integer_foreign_key_columns())
            .copied()
            .collect();

        for stmt in &self.stmts.stmts {
            let (_, original_start) = self
                .original
                .split_at_checked(stmt.stmt_location as usize)
                .ok_or(Error::StmtOutOfBounds)?;
            let (original, _) = original_start
                .split_at_checked(stmt.stmt_len as usize)
                .ok_or(Error::StmtOutOfBounds)?;

            if let Some(ref node) = stmt.stmt {
                if let Some(ref node) = node.node {
                    match node {
                        NodeEnum::CreateStmt(create_stmt) => {
                            let mut stmt = create_stmt.clone();
                            stmt.if_not_exists = true;

                            // Get table info
                            let (schema, table_name) =
                                if let Some(ref relation) = create_stmt.relation {
                                    (schema_name(relation), relation.relname.as_str())
                                } else {
                                    ("public", "")
                                };

                            // Convert integer PK/FK columns to bigint
                            for elt in &mut stmt.table_elts {
                                if let Some(NodeEnum::ColumnDef(ref mut col_def)) = elt.node {
                                    let col = Column {
                                        name: col_def.colname.as_str(),
                                        table: Some(table_name),
                                        schema: Some(schema),
                                    };

                                    if columns_to_convert.contains(&col) {
                                        if let Some(ref mut type_name) = col_def.type_name {
                                            type_name.names = vec![
                                                Node {
                                                    node: Some(NodeEnum::String(PgString {
                                                        sval: "pg_catalog".to_owned(),
                                                    })),
                                                },
                                                Node {
                                                    node: Some(NodeEnum::String(PgString {
                                                        sval: "int8".to_owned(),
                                                    })),
                                                },
                                            ];
                                        }
                                    }
                                }
                            }

                            if state == SyncState::PreData {
                                let sql = deparse_node(NodeEnum::CreateStmt(stmt))?;
                                let table = create_stmt
                                    .relation
                                    .as_ref()
                                    .map(Table::from)
                                    .unwrap_or_default();
                                result.push(Statement::Table { table, sql });
                            }
                        }

                        NodeEnum::CreateSeqStmt(stmt) => {
                            let mut stmt = stmt.clone();
                            stmt.if_not_exists = true;
                            let sql = deparse_node(NodeEnum::CreateSeqStmt(stmt))?;
                            if state == SyncState::PreData {
                                // Bring sequences over.
                                result.push(sql.into());
                            }
                        }

                        NodeEnum::CreateExtensionStmt(stmt) => {
                            let mut stmt = stmt.clone();
                            stmt.if_not_exists = true;
                            let sql = deparse_node(NodeEnum::CreateExtensionStmt(stmt))?;
                            if state == SyncState::PreData {
                                result.push(sql.into());
                            }
                        }

                        NodeEnum::CreateSchemaStmt(stmt) => {
                            let mut stmt = stmt.clone();
                            stmt.if_not_exists = true;
                            let sql = deparse_node(NodeEnum::CreateSchemaStmt(stmt))?;
                            if state == SyncState::PreData {
                                result.push(sql.into());
                            }
                        }

                        NodeEnum::AlterTableStmt(stmt) => {
                            for cmd in &stmt.cmds {
                                if let Some(NodeEnum::AlterTableCmd(ref cmd)) = cmd.node {
                                    match cmd.subtype() {
                                        AlterTableType::AtAddConstraint => {
                                            if let Some(ref def) = cmd.def {
                                                if let Some(NodeEnum::Constraint(ref cons)) =
                                                    def.node
                                                {
                                                    // Only allow primary key constraints.
                                                    if matches!(
                                                        cons.contype(),
                                                        ConstrType::ConstrPrimary
                                                            | ConstrType::ConstrNotnull
                                                            | ConstrType::ConstrNull
                                                    ) {
                                                        // Integer PKs are already tracked and converted
                                                        // to bigint in CreateStmt handler
                                                        if state == SyncState::PreData {
                                                            result.push(Statement::Other {
                                                                sql: original.to_string(),
                                                                idempotent: false,
                                                            });
                                                        }
                                                    } else if cons.contype()
                                                        == ConstrType::ConstrForeign
                                                    {
                                                        // FK columns referencing integer PKs are
                                                        // computed from fk_columns at the end
                                                        if state == SyncState::PostData {
                                                            result.push(Statement::Other {
                                                                sql: original.to_string(),
                                                                idempotent: false,
                                                            });
                                                        }
                                                    } else if state == SyncState::PostData {
                                                        result.push(Statement::Other {
                                                            sql: original.to_string(),
                                                            idempotent: false,
                                                        });
                                                    }
                                                }
                                            }
                                        }
                                        AlterTableType::AtAttachPartition => {
                                            match stmt.objtype() {
                                                // Index partitions need to be attached to indexes,
                                                // which we create in the post-data step.
                                                ObjectType::ObjectIndex => {
                                                    if state == SyncState::PostData {
                                                        result.push(Statement::Other {
                                                            sql: original.to_string(),
                                                            idempotent: false,
                                                        });
                                                    }
                                                }

                                                // Table partitions are attached in pre-data
                                                // after the partition tables are created.
                                                ObjectType::ObjectTable => {
                                                    if state == SyncState::PreData {
                                                        result.push(Statement::Other {
                                                            sql: original.to_string(),
                                                            idempotent: false,
                                                        });
                                                    }
                                                }

                                                _ => {
                                                    if state == SyncState::PreData {
                                                        result.push(Statement::Other {
                                                            sql: original.to_string(),
                                                            idempotent: false,
                                                        });
                                                    }
                                                }
                                            }
                                        }

                                        AlterTableType::AtColumnDefault => {
                                            if state == SyncState::PreData {
                                                result.push(original.into())
                                            }
                                        }

                                        AlterTableType::AtAddIdentity => {
                                            if state == SyncState::Cutover {
                                                if let Some(ref node) = cmd.def {
                                                    if let Some(NodeEnum::Constraint(
                                                        ref constraint,
                                                    )) = node.node
                                                    {
                                                        for option in &constraint.options {
                                                            if let Some(NodeEnum::DefElem(
                                                                ref elem,
                                                            )) = option.node
                                                            {
                                                                if elem.defname == "sequence_name" {
                                                                    if let Some(ref node) = elem.arg
                                                                    {
                                                                        if let Some(
                                                                            NodeEnum::List(
                                                                                ref list,
                                                                            ),
                                                                        ) = node.node
                                                                        {
                                                                            let table = Table::try_from(list).map_err(|_| Error::MissingEntity)?;
                                                                            let sequence =
                                                                                Sequence::from(
                                                                                    table,
                                                                                );
                                                                            let table = stmt
                                                                                .relation
                                                                                .as_ref()
                                                                                .map(Table::from);
                                                                            let schema = table
                                                                                .and_then(
                                                                                    |table| {
                                                                                        table.schema
                                                                                    },
                                                                                );
                                                                            let table = table.map(
                                                                                |table| table.name,
                                                                            );
                                                                            let column = Column {
                                                                                table,
                                                                                name: cmd
                                                                                    .name
                                                                                    .as_str(),
                                                                                schema,
                                                                            };
                                                                            let sql = sequence.setval_from_column(&column).map_err(|_| Error::MissingEntity)?;

                                                                            result.push(Statement::SequenceSetMax { sequence, sql });
                                                                        }
                                                                    }
                                                                }
                                                            }
                                                        }
                                                    }
                                                }
                                            } else if state == SyncState::PreData {
                                                result.push(original.into());
                                            }
                                        }
                                        // AlterTableType::AtChangeOwner => {
                                        //     continue; // Don't change owners, for now.
                                        // }
                                        _ => {
                                            if state == SyncState::PreData {
                                                result.push(original.into());
                                            }
                                        }
                                    }
                                }
                            }
                        }

                        NodeEnum::CreateTrigStmt(stmt) => {
                            let mut stmt = stmt.clone();
                            stmt.replace = true;

                            if state == SyncState::PreData {
                                result.push(deparse_node(NodeEnum::CreateTrigStmt(stmt))?.into());
                            }
                        }

                        // Skip these.
                        NodeEnum::CreatePublicationStmt(_)
                        | NodeEnum::CreateSubscriptionStmt(_)
                        | NodeEnum::AlterPublicationStmt(_)
                        | NodeEnum::AlterSubscriptionStmt(_) => (),

                        NodeEnum::AlterSeqStmt(stmt) => {
                            if matches!(state, SyncState::PreData | SyncState::Cutover) {
                                let sequence = stmt
                                    .sequence
                                    .as_ref()
                                    .map(Table::from)
                                    .ok_or(Error::MissingEntity)?;
                                let sequence = Sequence::from(sequence);
                                let column = stmt.options.first().ok_or(Error::MissingEntity)?;
                                let column =
                                    Column::try_from(column).map_err(|_| Error::MissingEntity)?;

                                if state == SyncState::PreData {
                                    result.push(Statement::SequenceOwner {
                                        column,
                                        sequence,
                                        sql: original,
                                    });
                                } else if state == SyncState::Cutover {
                                    let sql = sequence
                                        .setval_from_column(&column)
                                        .map_err(|_| Error::MissingEntity)?;
                                    result.push(Statement::SequenceSetMax { sequence, sql })
                                }
                            }
                        }

                        NodeEnum::IndexStmt(stmt) => {
                            if state == SyncState::PostData {
                                let sql = {
                                    let mut stmt = stmt.clone();
                                    stmt.concurrent = stmt
                                        .relation
                                        .as_ref()
                                        .map(|relation| relation.inh) // ONLY used for partitioned tables, which can't be created concurrently.
                                        .unwrap_or(false);
                                    stmt.if_not_exists = true;
                                    deparse_node(NodeEnum::IndexStmt(stmt))?
                                };

                                let table =
                                    stmt.relation.as_ref().map(Table::from).unwrap_or_default();

                                result.push(Statement::Index {
                                    table,
                                    name: stmt.idxname.as_str(),
                                    sql,
                                });
                            }
                        }

                        NodeEnum::ViewStmt(stmt) => {
                            let mut stmt = stmt.clone();
                            stmt.replace = true;

                            if state == SyncState::PreData {
                                result.push(Statement::Other {
                                    sql: deparse_node(NodeEnum::ViewStmt(stmt))?,
                                    idempotent: true,
                                });
                            }
                        }

                        NodeEnum::CreateTableAsStmt(stmt) => {
                            let mut stmt = stmt.clone();
                            stmt.if_not_exists = true;

                            if state == SyncState::PreData {
                                result.push(Statement::Other {
                                    sql: deparse_node(NodeEnum::CreateTableAsStmt(stmt))?,
                                    idempotent: true,
                                });
                            }
                        }

                        NodeEnum::CreateFunctionStmt(stmt) => {
                            let mut stmt = stmt.clone();
                            stmt.replace = true;

                            if state == SyncState::PreData {
                                result.push(Statement::Other {
                                    sql: deparse_node(NodeEnum::CreateFunctionStmt(stmt))?,
                                    idempotent: true,
                                });
                            }
                        }

                        NodeEnum::AlterOwnerStmt(stmt) => {
                            if stmt.object_type() != ObjectType::ObjectPublication
                                && state == SyncState::PreData
                            {
                                result.push(Statement::Other {
                                    sql: original.to_string(),
                                    idempotent: true,
                                });
                            }
                        }

                        NodeEnum::CreateEnumStmt(_)
                        | NodeEnum::CreateDomainStmt(_)
                        | NodeEnum::CompositeTypeStmt(_) => {
                            if state == SyncState::PreData {
                                result.push(Statement::Other {
                                    sql: original.to_owned(),
                                    idempotent: false,
                                });
                            }
                        }

                        NodeEnum::VariableSetStmt(_) => continue,
                        NodeEnum::SelectStmt(_) => continue,
                        _ => {
                            if state == SyncState::PreData {
                                result.push(original.into());
                            }
                        }
                    }
                }
            }
        }

        Ok(result)
    }

    /// Create objects in destination cluster.
    pub async fn restore(
        &self,
        dest: &Cluster,
        ignore_errors: bool,
        state: SyncState,
    ) -> Result<(), Error> {
        let stmts = self.statements(state)?;

        for (num, shard) in dest.shards().iter().enumerate() {
            let mut primary = shard.primary(&Request::default()).await?;

            info!(
                "syncing schema into shard {} [{}, {}]",
                num,
                primary.addr(),
                dest.name()
            );

            let mut progress = Progress::new(stmts.len());

            for stmt in &stmts {
                progress.next(stmt);
                if let Err(err) = primary.execute(stmt.deref()).await {
                    if let backend::Error::ExecutionError(ref err) = err {
                        let code = &err.code;

                        if let Statement::Other { idempotent, .. } = stmt {
                            if !idempotent {
                                if matches!(code.as_str(), "42P16" | "42710" | "42809" | "42P07") {
                                    warn!("entity already exists, skipping");
                                    continue;
                                } else if !ignore_errors {
                                    return Err(Error::Backend(backend::Error::ExecutionError(
                                        err.clone(),
                                    )));
                                } else {
                                    warn!("skipping: {}", err);
                                }
                            }
                        }
                    } else {
                        return Err(err.into());
                    }
                    if ignore_errors {
                        warn!("skipping: {}", err);
                    } else {
                        return Err(err.into());
                    }
                }
                progress.done();
            }
        }

        Ok(())
    }
}

#[cfg(test)]
mod test {
    use super::*;

    #[tokio::test]
    async fn test_pg_dump_execute() {
        let cluster = Cluster::new_test_single_shard(&config());
        let _pg_dump = PgDump::new(&cluster, "test_pg_dump_execute");
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
        let _parse = pg_query::parse(&PgDump::clean(&dump)).unwrap();
    }

    #[test]
    fn test_generated_identity() {
        let q = "ALTER TABLE public.users ALTER COLUMN id ADD GENERATED ALWAYS AS IDENTITY (
            SEQUENCE NAME public.users_id_seq
            START WITH 1
            INCREMENT BY 1
            NO MINVALUE
            NO MAXVALUE
            CACHE 1
        );";
        let parse = pg_query::parse(q).unwrap();
        let output = PgDumpOutput {
            stmts: parse.protobuf,
            original: q.to_string(),
        };
        let statements = output.statements(SyncState::Cutover).unwrap();
        match statements.first() {
            Some(Statement::SequenceSetMax { sequence, sql }) => {
                assert_eq!(sequence.table.name, "users_id_seq");
                assert_eq!(
                    sequence.table.schema().map(|schema| schema.name),
                    Some("public")
                );
                assert_eq!(
                    sql,
                    r#"SELECT setval('"public"."users_id_seq"', COALESCE((SELECT MAX("id") FROM "public"."users"), 1), true);"#
                );
            }

            _ => panic!("not a set sequence max"),
        }
        let statements = output.statements(SyncState::PreData).unwrap();
        assert!(!statements.is_empty());
        let statements = output.statements(SyncState::PostData).unwrap();
        assert!(statements.is_empty());
    }

    #[test]
    fn test_integer_primary_key_columns() {
        let query = r#"
CREATE TABLE users (id INTEGER, name TEXT);
ALTER TABLE users ADD CONSTRAINT users_pkey PRIMARY KEY (id);"#;

        let output = PgDumpOutput {
            stmts: parse(query).unwrap().protobuf,
            original: query.to_owned(),
        };

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
        let query = r#"
CREATE TABLE users (id UUID, name TEXT);
ALTER TABLE users ADD CONSTRAINT users_pkey PRIMARY KEY (id);"#;

        let output = PgDumpOutput {
            stmts: parse(query).unwrap().protobuf,
            original: query.to_owned(),
        };

        let pk_columns = output.integer_primary_key_columns();

        // UUID primary key should not be included
        assert_eq!(pk_columns.len(), 0);
    }

    #[test]
    fn test_integer_foreign_key_columns() {
        let query = r#"
CREATE TABLE parent (id INTEGER, name TEXT);
CREATE TABLE child (id INTEGER, parent_id INTEGER);
ALTER TABLE parent ADD CONSTRAINT parent_pkey PRIMARY KEY (id);
ALTER TABLE child ADD CONSTRAINT child_parent_fk FOREIGN KEY (parent_id) REFERENCES parent(id);"#;

        let output = PgDumpOutput {
            stmts: parse(query).unwrap().protobuf,
            original: query.to_owned(),
        };

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
        let query = r#"
CREATE TABLE parent (id1 INTEGER, id2 INTEGER, name TEXT);
CREATE TABLE child (id INTEGER, parent_id1 INTEGER, parent_id2 INTEGER);
ALTER TABLE parent ADD CONSTRAINT parent_pkey PRIMARY KEY (id1, id2);
ALTER TABLE child ADD CONSTRAINT child_parent_fk FOREIGN KEY (parent_id1, parent_id2) REFERENCES parent(id1, id2);"#;

        let output = PgDumpOutput {
            stmts: parse(query).unwrap().protobuf,
            original: query.to_owned(),
        };

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
        let query = r#"
CREATE TABLE order_items (order_id INTEGER, item_id INTEGER, quantity INTEGER);
ALTER TABLE order_items ADD CONSTRAINT order_items_pkey PRIMARY KEY (order_id, item_id);"#;

        let output = PgDumpOutput {
            stmts: parse(query).unwrap().protobuf,
            original: query.to_owned(),
        };

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
        let query = r#"
CREATE TABLE test (id INTEGER, value TEXT);
ALTER TABLE test ADD CONSTRAINT id_pkey PRIMARY KEY (id);"#;

        let output = PgDumpOutput {
            stmts: parse(query).unwrap().protobuf,
            original: query.to_owned(),
        };

        let statements = output.statements(SyncState::PreData).unwrap();
        assert_eq!(statements.len(), 2);

        // Integer PK column should be converted to bigint directly in CREATE TABLE
        assert_eq!(
            statements[0].deref(),
            "CREATE TABLE IF NOT EXISTS test (id bigint, value text)"
        );
        assert_eq!(
            statements[1].deref(),
            "\nALTER TABLE test ADD CONSTRAINT id_pkey PRIMARY KEY (id)"
        );
    }

    #[test]
    fn test_bigint_rewrite_foreign_key() {
        let query = r#"
CREATE TABLE parent (id INTEGER, name TEXT);
CREATE TABLE child (id INTEGER, parent_id INTEGER);
ALTER TABLE parent ADD CONSTRAINT parent_pkey PRIMARY KEY (id);
ALTER TABLE child ADD CONSTRAINT child_parent_fk FOREIGN KEY (parent_id) REFERENCES parent(id);"#;

        let output = PgDumpOutput {
            stmts: parse(query).unwrap().protobuf,
            original: query.to_owned(),
        };

        let statements = output.statements(SyncState::PreData).unwrap();
        assert_eq!(statements.len(), 3);

        // PK column converted to bigint in CREATE TABLE
        assert_eq!(
            statements[0].deref(),
            "CREATE TABLE IF NOT EXISTS parent (id bigint, name text)"
        );
        // FK column also converted to bigint in CREATE TABLE
        assert_eq!(
            statements[1].deref(),
            "CREATE TABLE IF NOT EXISTS child (id int, parent_id bigint)"
        );
        assert_eq!(
            statements[2].deref(),
            "\nALTER TABLE parent ADD CONSTRAINT parent_pkey PRIMARY KEY (id)"
        );
    }

    #[test]
    fn test_attach_partition() {
        // pg_dump generates ATTACH PARTITION for partitioned tables
        let query = r#"
CREATE TABLE parent (id INTEGER, created_at DATE) PARTITION BY RANGE (created_at);
CREATE TABLE parent_2024 (id INTEGER, created_at DATE);
ALTER TABLE ONLY parent ATTACH PARTITION parent_2024 FOR VALUES FROM ('2024-01-01') TO ('2025-01-01');"#;

        let output = PgDumpOutput {
            stmts: parse(query).unwrap().protobuf,
            original: query.to_owned(),
        };

        let pre_data = output.statements(SyncState::PreData).unwrap();
        let post_data = output.statements(SyncState::PostData).unwrap();

        // CREATE TABLEs should be in pre-data
        assert_eq!(pre_data.len(), 3);

        // ATTACH PARTITION for tables should be in pre-data, not post-data
        assert!(pre_data[2].deref().contains("ATTACH PARTITION"));

        // No statements in post-data for table partitions
        assert!(post_data.is_empty());
    }
}
