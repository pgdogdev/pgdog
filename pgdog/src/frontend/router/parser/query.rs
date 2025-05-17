//! Route queries to correct shards.
use std::{
    collections::{BTreeSet, HashSet},
    sync::Arc,
};

use crate::{
    backend::{databases::databases, replication::ShardedColumn, Cluster, ShardingSchema},
    config::{config, ReadWriteStrategy},
    frontend::{
        buffer::BufferedQuery,
        router::{
            context::RouterContext,
            parser::{rewrite::Rewrite, OrderBy, Shard},
            round_robin,
            sharding::{shard_param, shard_str, shard_value, Centroids},
            CopyRow,
        },
        PreparedStatements,
    },
    net::{
        messages::{Bind, CopyData, Vector},
        parameter::ParameterValue,
        Parameters,
    },
};

use super::*;

use multi_tenant::MultiTenantCheck;
use once_cell::sync::Lazy;
use pg_query::{
    fingerprint, parse,
    protobuf::{a_const::Val, *},
    NodeEnum,
};
use regex::Regex;
use tracing::{debug, trace};

static REPLICATION_REGEX: Lazy<Regex> = Lazy::new(|| {
    Regex::new(
        "(CREATE_REPLICATION_SLOT|IDENTIFY_SYSTEM|DROP_REPLICATION_SLOT|READ_REPLICATION_SLOT|ALTER_REPLICATION_SLOT|TIMELINE_HISTORY).*",
    )
    .unwrap()
});

#[derive(Debug)]
pub struct QueryParser {
    command: Command,
    replication_mode: bool,
    routed: bool,
    in_transaction: bool,
}

impl Default for QueryParser {
    fn default() -> Self {
        Self {
            command: Command::Query(Route::default()),
            replication_mode: false,
            routed: false,
            in_transaction: false,
        }
    }
}

impl QueryParser {
    /// Set parser to handle replication commands.
    pub fn replication_mode(&mut self) {
        self.replication_mode = true;
    }

    pub fn parse(&mut self, context: RouterContext) -> Result<&Command, Error> {
        if let Some(ref query) = context.query {
            self.command = self.query(
                query,
                context.cluster,
                context.bind,
                context.prepared_statements,
                context.params,
            )?;

            // If the cluster only has one shard, use direct-to-shard queries.
            if let Command::Query(ref mut query) = self.command {
                if !matches!(query.shard(), Shard::Direct(_)) && context.cluster.shards().len() == 1
                {
                    query.set_shard(0);
                }
            }
        }

        Ok(&self.command)
    }

    /// Shard copy data.
    pub fn copy_data(&mut self, rows: Vec<CopyData>) -> Result<Vec<CopyRow>, Error> {
        match &mut self.command {
            Command::Copy(copy) => copy.shard(rows),
            _ => Ok(vec![]),
        }
    }

    /// Get the route currently determined by the parser.
    pub fn route(&self) -> Route {
        match self.command {
            Command::Query(ref route) => route.clone(),
            _ => Route::write(None),
        }
    }

    /// Reset shard.
    pub fn reset(&mut self) {
        self.routed = false;
        self.in_transaction = false;
        self.command = Command::Query(Route::default());
    }

    fn query(
        &mut self,
        query: &BufferedQuery,
        cluster: &Cluster,
        bind: Option<&Bind>,
        prepared_statements: &mut PreparedStatements,
        params: &Parameters,
    ) -> Result<Command, Error> {
        // Replication protocol commands
        // don't have a node in pg_query,
        // so we have to parse them using a regex.
        if self.replication_mode {
            if query.starts_with("START_REPLICATION") {
                return Ok(Command::StartReplication);
            }

            if REPLICATION_REGEX.is_match(query) {
                return Ok(Command::ReplicationMeta);
            }
        }

        let shards = cluster.shards().len();
        let read_only = cluster.read_only();
        let write_only = cluster.write_only();
        let full_prepared_statements = config().config.general.prepared_statements.full();
        let sharding_schema = cluster.sharding_schema();
        let dry_run = sharding_schema.tables.dry_run();
        let multi_tenant = cluster.multi_tenant();
        let router_disabled = shards == 1 && (read_only || write_only);
        let parser_disabled =
            !full_prepared_statements && router_disabled && !dry_run && multi_tenant.is_none();
        let rw_strategy = cluster.read_write_strategy();

        debug!(
            "parser is {}",
            if parser_disabled {
                "disabled"
            } else {
                "enabled"
            }
        );

        // Don't use the parser if the cluster has only one shard
        // and only one kind of database (either primary or just replicas),
        // and we don't expect prepared statements to arrive over the simple protocol.
        //
        // We know what the routing decision is in this case and we don't
        // need to invoke the parser.
        if parser_disabled {
            if read_only {
                return Ok(Command::Query(Route::read(Some(0))));
            }
            if write_only {
                return Ok(Command::Query(Route::write(Some(0))));
            }
        }

        // We already decided where all queries for this
        // transaction are going to go.
        if self.routed && multi_tenant.is_none() {
            if dry_run {
                let cache = Cache::get();
                let route = self.route();
                cache.record_command(query, &route)?;
            }

            if multi_tenant.is_none() {
                return Ok(self.command.clone());
            }
        }

        let mut shard = Shard::All;

        // Parse hardcoded shard from a query comment.
        if !router_disabled && !self.routed {
            shard = super::comment::shard(query, &sharding_schema).map_err(Error::PgQuery)?;
        }

        // Cluster is read only or write only, traffic split isn't needed,
        // and prepared statements support is limited to the extended protocol,
        // don't parse the query further.
        if !full_prepared_statements && multi_tenant.is_none() {
            if let Shard::Direct(_) = shard {
                if cluster.read_only() {
                    return Ok(Command::Query(Route::read(shard)));
                }

                if cluster.write_only() {
                    return Ok(Command::Query(Route::write(shard)));
                }
            }
        }

        let cache = Cache::get();

        // Get the AST from cache or parse the statement live.
        let ast = match query {
            // Only prepared statements (or just extended) are cached.
            BufferedQuery::Prepared(query) => cache.parse(query.query()).map_err(Error::PgQuery)?,
            // Don't cache simple queries.
            //
            // They contain parameter values, which makes the cache
            // too large to be practical.
            //
            // Make your clients use prepared statements
            // or at least send statements with placeholders using the
            // extended protocol.
            BufferedQuery::Query(query) => Arc::new(parse(query.query()).map_err(Error::PgQuery)?),
        };

        debug!("{}", query.query());
        trace!("{:#?}", ast);

        let rewrite = Rewrite::new(ast.clone());
        if rewrite.needs_rewrite() {
            let queries = rewrite.rewrite(prepared_statements)?;
            return Ok(Command::Rewrite(queries));
        }

        if let Some(multi_tenant) = multi_tenant {
            debug!("running multi-tenant check");
            MultiTenantCheck::new(cluster.user(), multi_tenant, cluster.schema(), &ast, params)
                .run()?;
        }

        if self.routed {
            return Ok(self.command.clone());
        }

        //
        // Get the root AST node.
        //
        // We don't expect clients to send multiple queries. If they do
        // only the first one is used for routing.
        //
        let root = ast
            .protobuf
            .stmts
            .first()
            .ok_or(Error::EmptyQuery)?
            .stmt
            .as_ref()
            .ok_or(Error::EmptyQuery)?;

        let mut command = match root.node {
            // SELECT statements.
            Some(NodeEnum::SelectStmt(ref stmt)) => {
                if matches!(shard, Shard::Direct(_)) {
                    return Ok(Command::Query(Route::read(shard)));
                }
                // `SELECT NOW()`, `SELECT 1`, etc.
                else if ast.tables().is_empty() {
                    return Ok(Command::Query(Route::read(Some(
                        round_robin::next() % cluster.shards().len(),
                    ))));
                } else {
                    let mut command = Self::select(stmt, &sharding_schema, bind)?;
                    let mut omni = false;
                    if let Command::Query(query) = &mut command {
                        // Try to route an all-shard query to one
                        // shard if the table(s) it's touching contain
                        // the same data on all shards.
                        if query.is_all_shards() {
                            let tables = ast.tables();
                            omni = tables
                                .iter()
                                .all(|t| sharding_schema.tables.omnishards().contains(t));
                        }

                        if omni {
                            query.set_shard(round_robin::next() % cluster.shards().len());
                        }
                    }

                    Ok(command)
                }
            }
            // SET statements.
            Some(NodeEnum::VariableSetStmt(ref stmt)) => {
                return self.set(stmt, &sharding_schema, read_only)
            }
            // COPY statements.
            Some(NodeEnum::CopyStmt(ref stmt)) => Self::copy(stmt, cluster),
            // INSERT statements.
            Some(NodeEnum::InsertStmt(ref stmt)) => Self::insert(stmt, &sharding_schema, bind),
            // UPDATE statements.
            Some(NodeEnum::UpdateStmt(ref stmt)) => Self::update(stmt),
            // DELETE statements.
            Some(NodeEnum::DeleteStmt(ref stmt)) => Self::delete(stmt),
            // Transaction control statements,
            // e.g. BEGIN, COMMIT, etc.
            Some(NodeEnum::TransactionStmt(ref stmt)) => {
                // Only allow to intercept transaction statements
                // if they are using the simple protocol.
                if query.simple() {
                    // In conservative read write split mode,
                    // we don't assume anything about transaction contents
                    // and just send it to the primary.
                    //
                    // Only single-statement SELECT queries can be routed
                    // to a replica.
                    if *rw_strategy == ReadWriteStrategy::Conservative {
                        self.routed = true;
                        return Ok(Command::Query(Route::write(None)));
                    }

                    match stmt.kind() {
                        TransactionStmtKind::TransStmtCommit => {
                            return Ok(Command::CommitTransaction)
                        }
                        TransactionStmtKind::TransStmtRollback => {
                            return Ok(Command::RollbackTransaction)
                        }
                        TransactionStmtKind::TransStmtBegin
                        | TransactionStmtKind::TransStmtStart => {
                            self.in_transaction = true;
                            return Ok(Command::StartTransaction(query.clone()));
                        }
                        _ => Ok(Command::Query(Route::write(None))),
                    }
                } else {
                    Ok(Command::Query(Route::write(None)))
                }
            }
            // All others are not handled.
            // They are sent to all shards concurrently.
            _ => Ok(Command::Query(Route::write(None))),
        }?;

        self.routed = true;

        // Overwrite shard using shard we got from a comment, if any.
        if let Shard::Direct(shard) = shard {
            if let Command::Query(ref mut route) = command {
                route.set_shard(shard);
            }
        }

        // If we only have one shard, set it.
        //
        // If the query parser couldn't figure it out,
        // there is no point of doing a multi-shard query with only one shard
        // in the set.
        //
        if cluster.shards().len() == 1 && !dry_run {
            if let Command::Query(ref mut route) = command {
                route.set_shard(0);
            }
        }

        // Last ditch attempt to route a query to a specific shard.
        //
        // Looking through manual queries to see if we have any
        // with the fingerprint.
        //
        if let Command::Query(ref mut route) = command {
            if route.shard().all() {
                let databases = databases();
                // Only fingerprint the query if some manual queries are configured.
                // Otherwise, we're wasting time parsing SQL.
                if !databases.manual_queries().is_empty() {
                    let fingerprint = fingerprint(query).map_err(Error::PgQuery)?;
                    trace!("fingerprint: {}", fingerprint.hex);
                    let manual_route = databases.manual_query(&fingerprint.hex).cloned();

                    // TODO: check routing logic required by config.
                    if manual_route.is_some() {
                        route.set_shard(round_robin::next() % cluster.shards().len());
                    }
                }
            }
        }

        debug!("query router decision: {:#?}", command);

        if dry_run {
            let default_route = Route::write(None);
            cache.record_command(
                query,
                match &command {
                    Command::Query(ref route) => route,
                    _ => &default_route,
                },
            )?;
            Ok(command.dry_run())
        } else {
            Ok(command)
        }
    }

    /// Handle the SET command.
    ///
    /// We allow setting shard/sharding key manually outside
    /// the normal protocol flow. This command is not forwarded to the server.
    ///
    /// All other SETs change the params on the client and are eventually sent to the server
    /// when the client is connected to the server.
    fn set(
        &mut self,
        stmt: &VariableSetStmt,
        sharding_schema: &ShardingSchema,
        read_only: bool,
    ) -> Result<Command, Error> {
        match stmt.name.as_str() {
            "pgdog.shard" => {
                let node = stmt
                    .args
                    .first()
                    .ok_or(Error::SetShard)?
                    .node
                    .as_ref()
                    .ok_or(Error::SetShard)?;
                if let NodeEnum::AConst(AConst {
                    val: Some(a_const::Val::Ival(Integer { ival })),
                    ..
                }) = node
                {
                    self.routed = true;
                    return Ok(Command::Query(
                        Route::write(Some(*ival as usize)).set_read(read_only),
                    ));
                }
            }

            "pgdog.sharding_key" => {
                let node = stmt
                    .args
                    .first()
                    .ok_or(Error::SetShard)?
                    .node
                    .as_ref()
                    .ok_or(Error::SetShard)?;

                if let NodeEnum::AConst(AConst {
                    val: Some(Val::Sval(String { sval })),
                    ..
                }) = node
                {
                    let shard = shard_str(sval, sharding_schema, &vec![], 0);
                    self.routed = true;
                    return Ok(Command::Query(Route::write(shard).set_read(read_only)));
                }
            }

            // TODO: Handle SET commands for updating client
            // params without touching the server.
            name => {
                if !self.in_transaction {
                    let mut value = vec![];

                    for node in &stmt.args {
                        if let Some(NodeEnum::AConst(AConst { val: Some(val), .. })) = &node.node {
                            match val {
                                Val::Sval(String { sval }) => {
                                    value.push(sval.to_string());
                                }

                                Val::Ival(Integer { ival }) => {
                                    value.push(ival.to_string());
                                }

                                Val::Fval(Float { fval }) => {
                                    value.push(fval.to_string());
                                }

                                Val::Boolval(Boolean { boolval }) => {
                                    value.push(boolval.to_string());
                                }

                                _ => (),
                            }
                        }
                    }

                    match value.len() {
                        0 => (),
                        1 => {
                            return Ok(Command::Set {
                                name: name.to_string(),
                                value: ParameterValue::String(value.pop().unwrap()),
                            })
                        }
                        _ => {
                            return Ok(Command::Set {
                                name: name.to_string(),
                                value: ParameterValue::Tuple(value),
                            })
                        }
                    }
                }
            }
        }

        Ok(Command::Query(Route::write(Shard::All).set_read(read_only)))
    }

    fn select(
        stmt: &SelectStmt,
        sharding_schema: &ShardingSchema,
        params: Option<&Bind>,
    ) -> Result<Command, Error> {
        let order_by = Self::select_sort(&stmt.sort_clause, params);
        let mut shards = HashSet::new();
        let table_name = stmt
            .from_clause
            .first()
            .and_then(|node| {
                node.node.as_ref().map(|node| match node {
                    NodeEnum::RangeVar(var) => Some(if let Some(ref alias) = var.alias {
                        alias.aliasname.as_str()
                    } else {
                        var.relname.as_str()
                    }),
                    _ => None,
                })
            })
            .flatten();
        if let Some(where_clause) = WhereClause::new(table_name, &stmt.where_clause) {
            // Complexity: O(number of sharded tables * number of columns in the query)
            for table in sharding_schema.tables.tables() {
                let table_name = table.name.as_deref();
                let keys = where_clause.keys(table_name, &table.column);
                for key in keys {
                    match key {
                        Key::Constant(value) => {
                            shards.insert(shard_value(
                                &value,
                                &table.data_type,
                                sharding_schema.shards,
                                &table.centroids,
                                table.centroid_probes,
                            ));
                        }

                        Key::Parameter(param) => {
                            if let Some(params) = params {
                                if let Some(param) = params.parameter(param)? {
                                    shards.insert(shard_param(
                                        &param,
                                        table,
                                        sharding_schema.shards,
                                    ));
                                }
                            }
                        }

                        // Null doesn't help.
                        Key::Null => (),
                    }
                }
            }
        }
        // Shard by vector in ORDER BY clause.
        for order in &order_by {
            if let Some((vector, column_name)) = order.vector() {
                for table in sharding_schema.tables.tables() {
                    if &table.column == column_name
                        && (table.name.is_none() || table.name.as_deref() == table_name)
                    {
                        let centroids = Centroids::from(&table.centroids);
                        shards.insert(centroids.shard(
                            vector,
                            sharding_schema.shards,
                            table.centroid_probes,
                        ));
                    }
                }
            }
        }

        let shard = if shards.len() == 1 {
            shards.iter().next().cloned().unwrap()
        } else {
            let mut multi = vec![];
            let mut all = false;
            for shard in &shards {
                match shard {
                    Shard::All => {
                        all = true;
                        break;
                    }
                    Shard::Direct(v) => multi.push(*v),
                    Shard::Multi(m) => multi.extend(m),
                };
            }
            if all || shards.is_empty() {
                Shard::All
            } else {
                Shard::Multi(multi)
            }
        };

        let aggregates = Aggregate::parse(stmt)?;

        Ok(Command::Query(
            Route::select(shard, &order_by, &aggregates).with_lock(!stmt.locking_clause.is_empty()),
        ))
    }

    /// Parse the `ORDER BY` clause of a `SELECT` statement.
    fn select_sort(nodes: &[Node], params: Option<&Bind>) -> Vec<OrderBy> {
        let mut order_by = vec![];
        for clause in nodes {
            if let Some(NodeEnum::SortBy(ref sort_by)) = clause.node {
                let asc = matches!(sort_by.sortby_dir, 0..=2);
                let Some(ref node) = sort_by.node else {
                    continue;
                };
                let Some(ref node) = node.node else {
                    continue;
                };

                match node {
                    NodeEnum::AConst(aconst) => {
                        if let Some(Val::Ival(ref integer)) = aconst.val {
                            order_by.push(if asc {
                                OrderBy::Asc(integer.ival as usize)
                            } else {
                                OrderBy::Desc(integer.ival as usize)
                            });
                        }
                    }

                    NodeEnum::ColumnRef(column_ref) => {
                        // TODO: save the entire column and disambiguate
                        // when reading data with RowDescription as context.
                        let Some(field) = column_ref.fields.last() else {
                            continue;
                        };
                        if let Some(NodeEnum::String(ref string)) = field.node {
                            order_by.push(if asc {
                                OrderBy::AscColumn(string.sval.clone())
                            } else {
                                OrderBy::DescColumn(string.sval.clone())
                            });
                        }
                    }

                    NodeEnum::AExpr(expr) => {
                        if expr.kind() == AExprKind::AexprOp {
                            if let Some(node) = expr.name.first() {
                                if let Some(NodeEnum::String(String { sval })) = &node.node {
                                    match sval.as_str() {
                                        "<->" => {
                                            let mut vector: Option<Vector> = None;
                                            let mut column: Option<std::string::String> = None;

                                            for e in
                                                [&expr.lexpr, &expr.rexpr].iter().copied().flatten()
                                            {
                                                if let Ok(vec) = Value::try_from(&e.node) {
                                                    match vec {
                                                        Value::Placeholder(p) => {
                                                            if let Some(bind) = params {
                                                                if let Ok(Some(param)) =
                                                                    bind.parameter((p - 1) as usize)
                                                                {
                                                                    vector = param.vector();
                                                                }
                                                            }
                                                        }
                                                        Value::Vector(vec) => vector = Some(vec),
                                                        _ => (),
                                                    }
                                                };

                                                if let Ok(col) = Column::try_from(&e.node) {
                                                    column = Some(col.name.to_owned());
                                                }
                                            }

                                            if let Some(vector) = vector {
                                                if let Some(column) = column {
                                                    order_by.push(OrderBy::AscVectorL2Column(
                                                        column, vector,
                                                    ));
                                                }
                                            }
                                        }
                                        _ => continue,
                                    }
                                }
                            }
                        }
                    }

                    _ => continue,
                }
            }
        }

        order_by
    }

    fn copy(stmt: &CopyStmt, cluster: &Cluster) -> Result<Command, Error> {
        let parser = CopyParser::new(stmt, cluster)?;
        if let Some(parser) = parser {
            Ok(Command::Copy(Box::new(parser)))
        } else {
            Ok(Command::Query(Route::write(None)))
        }
    }

    fn insert(
        stmt: &InsertStmt,
        sharding_schema: &ShardingSchema,
        params: Option<&Bind>,
    ) -> Result<Command, Error> {
        let insert = Insert::new(stmt);
        let columns = insert
            .columns()
            .into_iter()
            .map(|column| column.name)
            .collect::<Vec<_>>();
        let mut shards = BTreeSet::new();
        let table = insert.table().unwrap().name;
        if let Some(sharded_table) = sharding_schema.tables.table(table) {
            if let Some(column) = ShardedColumn::from_sharded_table(sharded_table, &columns) {
                for tuple in insert.tuples() {
                    if let Some(value) = tuple.get(column.position) {
                        shards.insert(if let Some(bind) = params {
                            value.shard_placeholder(bind, sharding_schema, &column)
                        } else {
                            value.shard(sharding_schema, &column)
                        });
                    }
                }
            }
            match shards.len() {
                0 => Ok(Command::Query(Route::write(Some(
                    round_robin::next() % sharding_schema.shards,
                )))),
                1 => Ok(Command::Query(Route::write(shards.pop_last().unwrap()))),
                // TODO: support sending inserts to multiple shards.
                _ => Ok(Command::Query(Route::write(None))),
            }
        } else {
            Ok(Command::Query(Route::write(None)))
        }
    }

    fn update(_stmt: &UpdateStmt) -> Result<Command, Error> {
        Ok(Command::Query(Route::write(None)))
    }

    fn delete(_stmt: &DeleteStmt) -> Result<Command, Error> {
        Ok(Command::Query(Route::write(None)))
    }
}

#[cfg(test)]
mod test {

    use crate::net::{
        messages::{parse::Parse, Parameter},
        Format,
    };

    use super::{super::Shard, *};
    use crate::frontend::{Buffer, RouterContext};
    use crate::net::messages::Query;
    use crate::net::Parameters;

    macro_rules! command {
        ($query:expr) => {{
            let query = $query;
            let mut query_parser = QueryParser::default();
            let buffer = Buffer::from(vec![Query::new(query).into()]);
            let cluster = Cluster::new_test();
            let mut stmt = PreparedStatements::default();
            let params = Parameters::default();
            let context = RouterContext::new(&buffer, &cluster, &mut stmt, &params).unwrap();
            let command = query_parser.parse(context).unwrap().clone();

            (command, query_parser)
        }};
    }

    macro_rules! query {
        ($query:expr) => {{
            let query = $query;
            let (command, _) = command!(query);

            match command {
                Command::Query(query) => query,

                _ => panic!("should be a query"),
            }
        }};
    }

    macro_rules! parse {
        ($query: expr, $params: expr) => {
            parse!("", $query, $params)
        };

        ($name:expr, $query:expr, $params:expr, $codes:expr) => {{
            let parse = Parse::named($name, $query);
            let params = $params
                .into_iter()
                .map(|p| Parameter {
                    len: p.len() as i32,
                    data: p.to_vec(),
                })
                .collect::<Vec<_>>();
            let bind = Bind::test_params_codes($name, &params, $codes);
            let route = QueryParser::default()
                .parse(
                    RouterContext::new(
                        &Buffer::from(vec![parse.into(), bind.into()]),
                        &Cluster::new_test(),
                        &mut PreparedStatements::default(),
                        &Parameters::default(),
                    )
                    .unwrap(),
                )
                .unwrap()
                .clone();

            match route {
                Command::Query(query) => query,

                _ => panic!("should be a query"),
            }
        }};

        ($name:expr, $query:expr, $params: expr) => {
            parse!($name, $query, $params, &[])
        };
    }

    #[test]
    fn test_start_replication() {
        let query = Query::new(
            r#"START_REPLICATION SLOT "sharded" LOGICAL 0/1E2C3B0 (proto_version '4', origin 'any', publication_names '"sharded"')"#,
        );
        let mut buffer = Buffer::new();
        buffer.push(query.into());

        let mut query_parser = QueryParser::default();
        query_parser.replication_mode();

        let cluster = Cluster::default();

        let command = query_parser
            .parse(
                RouterContext::new(
                    &buffer,
                    &cluster,
                    &mut PreparedStatements::default(),
                    &Parameters::default(),
                )
                .unwrap(),
            )
            .unwrap();
        assert!(matches!(command, &Command::StartReplication));
    }

    #[test]
    fn test_replication_meta() {
        let query = Query::new(r#"IDENTIFY_SYSTEM"#);
        let mut buffer = Buffer::new();
        buffer.push(query.into());

        let mut query_parser = QueryParser::default();
        query_parser.replication_mode();

        let cluster = Cluster::default();

        let command = query_parser
            .parse(
                RouterContext::new(
                    &buffer,
                    &cluster,
                    &mut PreparedStatements::default(),
                    &Parameters::default(),
                )
                .unwrap(),
            )
            .unwrap();
        assert!(matches!(command, &Command::ReplicationMeta));
    }

    #[test]
    fn test_insert() {
        let route = parse!(
            "INSERT INTO sharded (id, email) VALUES ($1, $2)",
            ["11".as_bytes(), "test@test.com".as_bytes()]
        );
        assert_eq!(route.shard(), &Shard::direct(1));
    }

    #[test]
    fn test_order_by_vector() {
        let route = query!("SELECT * FROM embeddings ORDER BY embedding <-> '[1,2,3]'");
        let order_by = route.order_by().first().unwrap();
        assert!(order_by.asc());
        assert_eq!(
            order_by.vector().unwrap(),
            (
                &Vector::from(&[1.0, 2.0, 3.0][..]),
                &std::string::String::from("embedding")
            ),
        );

        let route = parse!(
            "SELECT * FROM embeddings ORDER BY embedding  <-> $1",
            ["[4.0,5.0,6.0]".as_bytes()]
        );
        let order_by = route.order_by().first().unwrap();
        assert!(order_by.asc());
        assert_eq!(
            order_by.vector().unwrap(),
            (
                &Vector::from(&[4.0, 5.0, 6.0][..]),
                &std::string::String::from("embedding")
            )
        );
    }

    #[test]
    fn test_parse_with_cast() {
        let route = parse!(
            "test",
            r#"SELECT sharded.id, sharded.value
    FROM sharded
    WHERE sharded.id = $1::INTEGER ORDER BY sharded.id"#,
            [[0, 0, 0, 1]],
            &[Format::Binary]
        );
        assert!(route.is_read());
        assert_eq!(route.shard(), &Shard::Direct(0))
    }

    #[test]
    fn test_select_for_update() {
        let route = query!("SELECT * FROM sharded WHERE id = $1 FOR UPDATE");
        assert!(route.is_write());
        assert!(matches!(route.shard(), Shard::All));

        let route = parse!(
            "SELECT * FROM sharded WHERE id = $1 FOR UPDATE",
            ["1".as_bytes()]
        );
        assert!(matches!(route.shard(), Shard::Direct(_)));
        assert!(route.is_write());
    }

    #[test]
    fn test_omni() {
        let q = "SELECT sharded_omni.* FROM sharded_omni WHERE sharded_omni.id = $1";
        let route = query!(q);
        assert!(matches!(route.shard(), Shard::Direct(_)));
        let (_, qp) = command!(q);
        assert!(qp.routed);
        assert!(!qp.in_transaction);
    }

    #[test]
    fn test_set() {
        let route = query!(r#"SET "pgdog.shard" TO 1"#);
        assert_eq!(route.shard(), &Shard::Direct(1));
        let (_, qp) = command!(r#"SET "pgdog.shard" TO 1"#);
        assert!(qp.routed);
        assert!(!qp.in_transaction);

        let route = query!(r#"SET "pgdog.sharding_key" TO '11'"#);
        assert_eq!(route.shard(), &Shard::Direct(1));
        let (_, qp) = command!(r#"SET "pgdog.sharding_key" TO '11'"#);
        assert!(qp.routed);
        assert!(!qp.in_transaction);

        for (command, qp) in [
            command!("SET TimeZone TO 'UTC'"),
            command!("SET TIME ZONE 'UTC'"),
        ] {
            match command {
                Command::Set { name, value } => {
                    assert_eq!(name, "timezone");
                    assert_eq!(value, ParameterValue::from("UTC"));
                }
                _ => panic!("not a set"),
            };
            assert!(!qp.routed);
            assert!(!qp.in_transaction);
        }

        let (command, qp) = command!("SET statement_timeout TO 3000");
        match command {
            Command::Set { name, value } => {
                assert_eq!(name, "statement_timeout");
                assert_eq!(value, ParameterValue::from("3000"));
            }
            _ => panic!("not a set"),
        };
        assert!(!qp.routed);
        assert!(!qp.in_transaction);

        // TODO: user shouldn't be able to set these.
        // The server will report an error on synchronization.
        let (command, qp) = command!("SET is_superuser TO true");
        match command {
            Command::Set { name, value } => {
                assert_eq!(name, "is_superuser");
                assert_eq!(value, ParameterValue::from("true"));
            }
            _ => panic!("not a set"),
        };
        assert!(!qp.routed);
        assert!(!qp.in_transaction);

        let (_, mut qp) = command!("BEGIN");
        let command = qp
            .parse(
                RouterContext::new(
                    &vec![Query::new(r#"SET statement_timeout TO 3000"#).into()].into(),
                    &Cluster::new_test(),
                    &mut PreparedStatements::default(),
                    &Parameters::default(),
                )
                .unwrap(),
            )
            .unwrap();
        match command {
            Command::Query(q) => assert!(q.is_write()),
            _ => panic!("set should trigger binding"),
        }

        let (command, _) = command!("SET search_path TO \"$user\", public, \"APPLES\"");
        match command {
            Command::Set { name, value } => {
                assert_eq!(name, "search_path");
                assert_eq!(
                    value,
                    ParameterValue::Tuple(vec!["$user".into(), "public".into(), "APPLES".into()])
                )
            }
            _ => panic!("search path"),
        }

        let ast = parse("SET statement_timeout TO 1").unwrap();
        let mut qp = QueryParser {
            in_transaction: true,
            ..Default::default()
        };

        let root = ast.protobuf.stmts.first().unwrap().stmt.as_ref().unwrap();
        match root.node.as_ref() {
            Some(NodeEnum::VariableSetStmt(stmt)) => {
                for read_only in [true, false] {
                    let route = qp.set(stmt, &ShardingSchema::default(), read_only).unwrap();
                    match route {
                        Command::Query(route) => {
                            assert_eq!(route.is_read(), read_only);
                        }
                        _ => panic!("not a query"),
                    }
                }
            }

            _ => panic!("not a set"),
        }
    }

    #[test]
    fn test_transaction() {
        let (command, qp) = command!("BEGIN");
        match command {
            Command::Query(q) => assert!(q.is_write()),
            _ => panic!("not a query"),
        };

        assert!(qp.routed);
        assert!(!qp.in_transaction);

        let mut cluster = Cluster::new_test();
        cluster.set_read_write_strategy(ReadWriteStrategy::Aggressive);

        let mut qp = QueryParser::default();
        let command = qp
            .query(
                &BufferedQuery::Query(Query::new("BEGIN")),
                &cluster,
                None,
                &mut PreparedStatements::default(),
                &Parameters::default(),
            )
            .unwrap();
        assert!(matches!(
            command,
            Command::StartTransaction(BufferedQuery::Query(_))
        ));
        assert!(!qp.routed);
        assert!(qp.in_transaction);

        let route = qp
            .query(
                &BufferedQuery::Query(Query::new("SET application_name TO 'test'")),
                &cluster,
                None,
                &mut PreparedStatements::default(),
                &Parameters::default(),
            )
            .unwrap();

        match route {
            Command::Query(q) => {
                assert!(q.is_read());
                assert!(cluster.read_only());
            }

            _ => panic!("not a query"),
        }
    }

    #[test]
    fn test_insert_do_update() {
        let route = query!("INSERT INTO foo (id) VALUES ($1::UUID) ON CONFLICT (id) DO UPDATE SET id = excluded.id RETURNING id");
        assert!(route.is_write())
    }

    #[test]
    fn test_begin_extended() {
        let mut qr = QueryParser::default();
        let result = qr
            .parse(
                RouterContext::new(
                    &vec![crate::net::Parse::new_anonymous("BEGIN").into()].into(),
                    &Cluster::new_test(),
                    &mut PreparedStatements::default(),
                    &Parameters::default(),
                )
                .unwrap(),
            )
            .unwrap();
        assert!(matches!(result, Command::Query(_)));
    }
}
