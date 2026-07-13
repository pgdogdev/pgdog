#[cfg(feature = "new_parser")]
use itertools::*;
use once_cell::sync::OnceCell;
use pg_query::{NodeEnum, ParseResult, parse, parse_raw};
#[cfg(feature = "new_parser")]
use pg_raw_parse::{Owned, StmtList, make};
use pgdog_config::QueryParserEngine;
use std::fmt::Debug;
use std::ops::Deref;
use std::time::Instant;

use parking_lot::Mutex;
use std::sync::Arc;
use tracing::warn;

use super::super::{Error, Route, Shard, StatementRewrite, StatementRewriteContext};
use super::{Cache, Fingerprint, Stats};
use crate::backend::schema::Schema;
use crate::frontend::PreparedStatements;
use crate::frontend::router::parser::cache::AstQuery;
use crate::frontend::router::parser::rewrite::statement::RewritePlan;
use crate::net::parameter::ParameterValue;
use crate::{backend::ShardingSchema, config::Role};

/// Abstract syntax tree (query) cache entry,
/// with statistics.
#[derive(Debug, Clone)]
pub struct Ast {
    /// Was this entry cached?
    pub cached: bool,
    /// Shard.
    pub comment_shard: Option<Shard>,
    /// Role.
    pub comment_role: Option<Role>,
    /// Parser query engine used.
    pub query_parser_engine: QueryParserEngine,
    /// Inner sync.
    inner: Arc<AstInner>,
}

#[derive(Debug)]
pub struct AstInner {
    /// Cached AST.
    pub ast: ParseResult,
    // FIXME(sage): Rename to ast when parser port is done
    #[cfg(feature = "new_parser")]
    pub new_ast: Owned<StmtList>,
    /// AST stats.
    pub stats: Mutex<Stats>,
    /// Rewrite plan.
    pub rewrite_plan: RewritePlan,
    /// Fingerprint.
    pub fingerprint: OnceCell<Fingerprint>,
    /// Original query.
    pub query_without_comment: Arc<str>,
}

impl AstInner {
    /// Create new AST record, with no rewrite or comment routing.
    #[cfg(feature = "new_parser")]
    pub(crate) fn new(ast: Owned<StmtList>) -> Self {
        let deparse_results = ast
            .iter()
            .map(|s| pg_raw_parse::deparse(s))
            .collect::<Result<Vec<_>, _>>()
            .unwrap();
        Self {
            new_ast: ast,
            ast: pg_query::parse(&deparse_results.iter().map(|r| r.as_str()).join("; ")).unwrap(),
            stats: Mutex::new(Stats::new()),
            rewrite_plan: RewritePlan::default(),
            fingerprint: OnceCell::new(),
            query_without_comment: "".into(),
        }
    }

    pub(crate) fn old(ast: ParseResult) -> Self {
        Self {
            #[cfg(feature = "new_parser")]
            new_ast: pg_raw_parse::parse(&ast.deparse().unwrap())
                .unwrap()
                .into_inner(),
            ast,
            stats: Mutex::new(Stats::new()),
            rewrite_plan: RewritePlan::default(),
            fingerprint: OnceCell::new(),
            query_without_comment: "".into(),
        }
    }
}

impl Deref for Ast {
    type Target = AstInner;

    fn deref(&self) -> &Self::Target {
        &self.inner
    }
}

impl Ast {
    /// Parse statement and run the rewrite engine, if necessary.
    pub(super) fn new(
        query: &AstQuery,
        schema: &ShardingSchema,
        db_schema: &Schema,
        prepared_statements: &mut PreparedStatements,
        user: &str,
        search_path: Option<&ParameterValue>,
    ) -> Result<Self, Error> {
        let now = Instant::now();
        #[cfg(not(feature = "new_parser"))]
        let mut ast = match schema.query_parser_engine {
            QueryParserEngine::PgQueryProtobuf => parse(query.query_without_comment),
            QueryParserEngine::PgQueryRaw => parse_raw(query.query_without_comment),
        }
        .map_err(Error::PgQuery)?;
        #[cfg(feature = "new_parser")]
        let ast = pg_raw_parse::parse(query.query_without_comment).map_err(Error::Parse)?;

        // Run the rewrite unconditionally. Even when a shard comment will
        // route the query to a specific shard, we need to know whether the
        // same query body (without the comment) would require a rewrite, so
        // `Cache::query` can decide whether this entry is safe to cache.
        let mut rewriter = StatementRewrite::new(StatementRewriteContext {
            #[cfg(not(feature = "new_parser"))]
            stmt: &mut ast.protobuf,
            extended: query.original_query.extended(),
            prepared: query.original_query.prepared(),
            prepared_statements,
            schema,
            db_schema,
            user,
            search_path,
        });
        #[cfg(feature = "new_parser")]
        let mut rewrite_plan = Default::default();
        #[cfg(feature = "new_parser")]
        let ast = make::try_owned(|mem| {
            // FIXME(sage): We should have a parse function on mem so we don't
            // need to parse and then copy the parsed tree just to throw the
            // original away
            let mut copy = mem.make_unique(&*ast.into_inner());
            if let Some(stmt) = copy.as_mut().into_iter().next() {
                rewrite_plan = rewriter.maybe_rewrite(stmt, mem)?;
            }
            Ok::<_, Error>(copy)
        })?;

        #[cfg(not(feature = "new_parser"))]
        let rewrite_plan = rewriter.maybe_rewrite()?;

        let elapsed = now.elapsed();
        let mut stats = Stats::new();
        stats.parse_time += elapsed;

        if let Some(threshold) = schema.log_min_duration_parse
            && elapsed >= threshold
        {
            warn!(
                "[slow_query_parse] parse_time_in_ms={}ms truncated_query=\"{}\"",
                elapsed.as_millis(),
                query.truncated_query(schema.log_query_sample_length),
            );
        }

        #[cfg(feature = "new_parser")]
        let old_ast =
            pg_query::parse(&pg_raw_parse::deparse_stmts(&*ast)?).map_err(Error::PgQuery)?;

        Ok(Self {
            cached: true,
            comment_shard: None,
            comment_role: None,
            query_parser_engine: schema.query_parser_engine,
            inner: Arc::new(AstInner {
                stats: Mutex::new(stats),
                #[cfg(feature = "new_parser")]
                new_ast: ast,
                #[cfg(feature = "new_parser")]
                ast: old_ast,
                #[cfg(not(feature = "new_parser"))]
                ast,
                rewrite_plan,
                fingerprint: OnceCell::new(),
                query_without_comment: query.query_without_comment.into(),
            }),
        })
    }

    /// Parse statement using AstContext for schema and user information.
    pub(super) fn with_context(
        query: &AstQuery,
        ctx: &super::AstContext<'_>,
        prepared_statements: &mut PreparedStatements,
    ) -> Result<Self, Error> {
        Self::new(
            query,
            &ctx.sharding_schema,
            &ctx.db_schema,
            prepared_statements,
            ctx.user,
            ctx.search_path,
        )
    }

    /// Record new AST entry, without rewriting or comment-routing.
    pub fn new_record(query: &str, query_parser_engine: QueryParserEngine) -> Result<Self, Error> {
        let ast = match query_parser_engine {
            QueryParserEngine::PgQueryProtobuf => parse(query),
            QueryParserEngine::PgQueryRaw => parse_raw(query),
        }
        .map_err(Error::PgQuery)?;

        Ok(Self {
            cached: true,
            comment_role: None,
            comment_shard: None,
            query_parser_engine,
            inner: Arc::new(AstInner::old(ast)),
        })
    }

    /// Create new AST from a parse result.
    #[cfg(feature = "new_parser")]
    pub fn from_raw_stmts(stmts: Owned<StmtList>) -> Self {
        Self {
            cached: true,
            comment_role: None,
            comment_shard: None,
            query_parser_engine: QueryParserEngine::default(),
            inner: Arc::new(AstInner::new(stmts)),
        }
    }

    /// Create new AST from a parse result.
    pub fn from_parse_result(parse_result: ParseResult) -> Self {
        Self {
            cached: true,
            comment_role: None,
            comment_shard: None,
            query_parser_engine: QueryParserEngine::default(),
            inner: Arc::new(AstInner::old(parse_result)),
        }
    }

    /// Get the reference to the AST.
    pub fn parse_result(&self) -> &ParseResult {
        &self.ast
    }

    /// Update stats for this statement, given the route
    /// calculated by the query parser.
    pub fn update_stats(&self, route: &Route) {
        let mut guard = self.stats.lock();

        if route.is_cross_shard() {
            guard.multi += 1;
        } else {
            guard.direct += 1;
        }
    }

    /// Get statement type.
    pub fn statement_type(&self) -> StatementType {
        let root = self
            .ast
            .protobuf
            .stmts
            .first()
            .and_then(|s| s.stmt.as_ref())
            .and_then(|s| s.node.as_ref());

        match root {
            Some(NodeEnum::SelectStmt(_))
            | Some(NodeEnum::InsertStmt(_))
            | Some(NodeEnum::UpdateStmt(_))
            | Some(NodeEnum::DeleteStmt(_))
            | Some(NodeEnum::CopyStmt(_))
            | Some(NodeEnum::ExplainStmt(_))
            | Some(NodeEnum::TransactionStmt(_)) => StatementType::Dml,

            Some(NodeEnum::VariableSetStmt(_))
            | Some(NodeEnum::VariableShowStmt(_))
            | Some(NodeEnum::DeallocateStmt(_))
            | Some(NodeEnum::ListenStmt(_))
            | Some(NodeEnum::NotifyStmt(_))
            | Some(NodeEnum::UnlistenStmt(_))
            | Some(NodeEnum::DiscardStmt(_)) => StatementType::Session,

            _ => StatementType::Ddl,
        }
    }

    /// Get a pre-computed fingerprint, or compute it again.
    pub fn fingerprint(&self) -> Result<&Fingerprint, Error> {
        if let Some(fingerprint) = self.fingerprint.get() {
            return Ok(fingerprint);
        }

        let start = Instant::now();
        let fingerprint = Fingerprint::new(&self.query_without_comment, self.query_parser_engine)
            .map_err(Error::PgQuery)?;
        let elapsed = start.elapsed();

        // try_insert is non-blocking: if another thread beat us to it, we
        // return their value and drop ours. Only the winner bumps stats so
        // we don't double-count under contention.
        match self.fingerprint.try_insert(fingerprint) {
            Ok(inserted) => {
                let mut my_stats = self.stats.lock();
                my_stats.parse_time += elapsed;
                my_stats.fingerprints += 1;
                drop(my_stats);

                let cache = Cache::get();
                let mut lock = cache.lock();
                lock.stats.parse_time += elapsed;
                lock.stats.fingerprints += 1;

                Ok(inserted)
            }
            Err((existing, _)) => Ok(existing),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub enum StatementType {
    Ddl,
    Dml,
    Session,
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::backend::ShardingSchema;
    use crate::backend::schema::Schema;
    use crate::frontend::BufferedQuery;
    use crate::net::Query;

    fn make_ast(sql: &str) -> Ast {
        let buffered = BufferedQuery::Query(Query::new(sql));
        Ast::new(
            &AstQuery::from_query(&buffered),
            &ShardingSchema::default(),
            &Schema::default(),
            &mut PreparedStatements::default(),
            "",
            None,
        )
        .unwrap()
    }

    #[test]
    fn test_fingerprint_returns_ok() {
        let ast = make_ast("SELECT 1");
        ast.fingerprint().unwrap();
    }

    #[test]
    fn test_fingerprint_is_cached() {
        let ast = make_ast("SELECT 2");
        let fp1 = ast.fingerprint().unwrap() as *const _;
        let fp2 = ast.fingerprint().unwrap() as *const _;
        // Second call should return the cached reference (same address).
        assert_eq!(fp1, fp2);
    }

    #[test]
    fn test_fingerprint_same_for_equivalent_queries() {
        let a = make_ast("SELECT 1 FROM users WHERE id = 1");
        let b = make_ast("SELECT 1 FROM users WHERE id = 2");
        let fp_a = a.fingerprint().unwrap();
        let fp_b = b.fingerprint().unwrap();
        assert_eq!(
            fp_a.value, fp_b.value,
            "queries differing only in constants should have the same fingerprint"
        );
    }

    #[test]
    fn test_fingerprint_differs_for_different_queries() {
        let a = make_ast("SELECT 1 FROM users");
        let b = make_ast("INSERT INTO users VALUES (1)");
        let fp_a = a.fingerprint().unwrap();
        let fp_b = b.fingerprint().unwrap();
        assert_ne!(
            fp_a.value, fp_b.value,
            "structurally different queries should have different fingerprints"
        );
    }

    #[test]
    fn test_fingerprint_updates_statement_parse_time() {
        let ast = make_ast("SELECT 1 FROM fp_stmt_time");
        let before = ast.stats.lock().parse_time;
        ast.fingerprint().unwrap();
        let after = ast.stats.lock().parse_time;
        assert!(
            after > before,
            "fingerprint() must bump per-statement parse_time"
        );
    }

    #[test]
    fn test_fingerprint_updates_global_parse_time() {
        let ast = make_ast("SELECT 1 FROM fp_global_time");
        let before = Cache::stats().0.parse_time;
        ast.fingerprint().unwrap();
        let after = Cache::stats().0.parse_time;
        assert!(
            after > before,
            "fingerprint() must bump global cache parse_time"
        );
    }

    #[test]
    fn test_fingerprint_cached_does_not_update_parse_time() {
        let ast = make_ast("SELECT 1 FROM fp_no_double_bump");
        // First call — computes and caches.
        ast.fingerprint().unwrap();
        let stmt_after_first = ast.stats.lock().parse_time;
        let global_after_first = Cache::stats().0.parse_time;

        // Second call — returns cached, should NOT bump times.
        ast.fingerprint().unwrap();
        let stmt_after_second = ast.stats.lock().parse_time;
        let global_after_second = Cache::stats().0.parse_time;

        assert_eq!(
            stmt_after_first, stmt_after_second,
            "cached fingerprint must not bump per-statement parse_time"
        );
        assert_eq!(
            global_after_first, global_after_second,
            "cached fingerprint must not bump global parse_time"
        );
    }

    #[test]
    fn test_fingerprint_increments_statement_fingerprints_counter() {
        let ast = make_ast("SELECT 1 FROM fp_stmt_counter");
        assert_eq!(ast.stats.lock().fingerprints, 0);
        ast.fingerprint().unwrap();
        assert_eq!(ast.stats.lock().fingerprints, 1);
    }

    #[test]
    fn test_fingerprint_increments_global_fingerprints_counter() {
        let ast = make_ast("SELECT 1 FROM fp_global_counter");
        let before = Cache::stats().0.fingerprints;
        ast.fingerprint().unwrap();
        let after = Cache::stats().0.fingerprints;
        // Other tests share the global counter under parallel execution,
        // so only assert it moved forward.
        assert!(
            after > before,
            "fingerprint() must increment global fingerprints counter"
        );
    }

    #[test]
    fn test_fingerprint_cached_does_not_increment_counters() {
        let ast = make_ast("SELECT 1 FROM fp_no_double_count");
        ast.fingerprint().unwrap();
        let stmt_after_first = ast.stats.lock().fingerprints;
        let global_after_first = Cache::stats().0.fingerprints;

        // Second call — cached, counters must not change.
        ast.fingerprint().unwrap();
        assert_eq!(
            ast.stats.lock().fingerprints,
            stmt_after_first,
            "cached fingerprint must not increment per-statement counter"
        );
        assert_eq!(
            Cache::stats().0.fingerprints,
            global_after_first,
            "cached fingerprint must not increment global counter"
        );
    }
}
