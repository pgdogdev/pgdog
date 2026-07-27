use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::time::Duration;

use pgdog_config::PoolerMode;
use tracing::{trace, warn};

use crate::backend::Cluster;
use crate::frontend::router::round_robin;
use crate::frontend::router::sharding::{LookupCache, MapLookup, PendingLookup, ResolveClaim};
use crate::net::messages::DataRow;
use crate::util::safe_timeout;

use super::*;

/// How long a client waits for another client's in-flight lookup table
/// load before failing the statement. Also breaks any deadlock between
/// waiters holding pool connections and the loading client needing one.
const LOOKUP_WAIT_TIMEOUT: Duration = Duration::from_secs(5);

/// How many times a load or a routing pass retries because the lookup
/// table was invalidated mid-flight, before failing the statement.
const LOOKUP_LOAD_ATTEMPTS: usize = 3;

#[derive(Debug, Clone)]
pub enum ClusterCheck {
    Ok,
    Offline,
}

impl QueryEngine {
    /// Get mutable reference to the backend connection.
    pub fn backend(&mut self) -> &mut Connection {
        &mut self.backend
    }

    /// Check that the cluster is still valid and online.
    pub async fn cluster_check(
        &mut self,
        context: &mut QueryEngineContext<'_>,
    ) -> Result<ClusterCheck, Error> {
        // Admin doesn't have a cluster.
        let res = match self.backend.cluster() {
            Ok(cluster) => {
                if !context.in_transaction() && !cluster.online() {
                    let identifier = cluster.identifier();

                    // Reload cluster config.
                    self.backend.safe_reload().await?;

                    if self.backend.cluster().is_ok() {
                        Ok(ClusterCheck::Ok)
                    } else {
                        self.error_response(
                            context,
                            ErrorResponse::connection(&identifier.user, &identifier.database),
                        )
                        .await?;
                        Ok(ClusterCheck::Offline)
                    }
                } else {
                    Ok(ClusterCheck::Ok)
                }
            }
            _ => Ok(ClusterCheck::Ok),
        };

        if let Ok(ClusterCheck::Ok) = res {
            // Wait for boot-time maintenance before we throw traffic at the cluster.
            if let Ok(cluster) = self.backend.cluster() {
                safe_timeout(
                    context.timeouts.query_timeout(&State::Active),
                    cluster.wait_ready(),
                )
                .await
                .map_err(|_| Error::ClusterStart)?;
            }
            res
        } else {
            res
        }
    }

    pub(super) async fn route_query(
        &mut self,
        context: &mut QueryEngineContext<'_>,
    ) -> Result<bool, Error> {
        // Check that we can route this transaction at all.
        if self.backend.pooler_mode() == PoolerMode::Statement && context.client_request.is_begin()
        {
            self.error_response(context, ErrorResponse::transaction_statement_mode())
                .await?;
            return Ok(false);
        }

        let cluster = match self.backend.cluster() {
            Ok(cluster) => cluster,
            _ => {
                return Ok(true);
            }
        };

        let router_context = RouterContext::new(
            context.client_request,
            cluster,
            context.params,
            context.transaction,
            context.sticky,
        )?;
        let mut result = self.router.query(router_context).map(|_| ());

        // Satisfy sharding key lookups recorded during routing and route
        // the query again with the loaded map. A statement never executes
        // while lookups are unresolved: the map can be invalidated between
        // the load and the re-route, and routing by the untranslated value
        // would put the statement on the wrong shard. Bounded, so repeated
        // invalidation fails the statement instead of looping.
        let mut attempts = 0;
        while result.is_ok() {
            let pending = self.router.take_pending_lookups();
            if pending.is_empty() {
                break;
            }

            attempts += 1;
            if attempts > LOOKUP_LOAD_ATTEMPTS {
                self.error_response(
                    context,
                    ErrorResponse::sharding_key_lookup(
                        &pending[0].lookup,
                        "lookup table invalidated repeatedly during routing",
                    ),
                )
                .await?;
                return Ok(false);
            }

            match Self::resolve_pending_lookups(cluster, pending).await {
                Ok(()) => {
                    let router_context = RouterContext::new(
                        context.client_request,
                        cluster,
                        context.params,
                        context.transaction,
                        context.sticky,
                    )?;
                    result = self.router.query(router_context).map(|_| ());
                }

                Err(response) => {
                    self.error_response(context, response).await?;
                    return Ok(false);
                }
            }
        }

        match result {
            Ok(()) => {
                // Lookup tables this statement writes to are flushed from
                // the cache when the write completes.
                let written = self.router.take_written_lookups();
                if !written.is_empty() && LookupCache::get().writes_paused() {
                    self.error_response(context, ErrorResponse::lookup_writes_paused(&written[0]))
                        .await?;
                    return Ok(false);
                }
                for lookup in written {
                    if !self.lookup_invalidations.contains(&lookup) {
                        self.lookup_invalidations.push(lookup);
                    }
                }

                let command = self.router.command();
                context.client_request.route = Some(command.route().clone());
                trace!(
                    "routing {:#?} to {:#?}",
                    context.client_request.messages, command,
                );

                // Apply post-parser rewrites, e.g. offset/limit.
                if let Some(rewrite_result) = &context.rewrite_result {
                    rewrite_result.apply_after_parser(context.client_request)?;
                }

                // Only validate shard placement for requests that actually execute
                // a query. Bare protocol-control batches (e.g. a lone Sync or Flush)
                // route to a default/cross-shard target but must still be forwarded
                // to the already-connected backend to finish the exchange.
                if context.client_request.is_executable() {
                    if Self::is_omnishard_unsafe(&self.backend, command, cluster) {
                        self.error_response(context, ErrorResponse::omni_in_direct_to_shard())
                            .await?;
                        return Ok(false);
                    }

                    if Self::is_shard_switch(command, &self.backend) {
                        self.error_response(context, ErrorResponse::direct_shard_mismatch())
                            .await?;
                        return Ok(false);
                    }
                }
            }
            Err(err) => {
                self.error_response(context, ErrorResponse::syntax(err.to_string().as_str()))
                    .await?;

                return Ok(false);
            }
        }

        Ok(true)
    }

    /// Satisfy sharding key lookups recorded during routing: load lookup
    /// table maps that aren't in memory, and verify values missing from
    /// maps old enough that the row may have been added since (e.g.
    /// through another PgDog instance). Loads run against a single shard
    /// picked round-robin; the lookup table is required to be
    /// omnisharded, so any shard is authoritative.
    ///
    /// Every pending lookup translates when this returns `Ok`. Anything
    /// else is an error for the client: the lookup table must have a row
    /// for every routed value, and routing by the untranslated value
    /// would put the statement on the wrong shard.
    async fn resolve_pending_lookups(
        cluster: &Cluster,
        pending: Vec<PendingLookup>,
    ) -> Result<(), ErrorResponse> {
        // A statement can extract the same value more than once,
        // e.g. a multi-row INSERT with repeated values.
        let pending = pending.into_iter().collect::<HashSet<_>>();

        let shards = cluster.shards();
        if shards.is_empty() {
            if let Some(lookup) = pending.iter().next() {
                return Err(ErrorResponse::sharding_key_lookup(
                    &lookup.lookup,
                    "no shards available",
                ));
            }
            return Ok(());
        }

        let cache = LookupCache::get();

        for lookup in pending {
            // The map could have loaded while this statement was parsed.
            if Self::lookup_satisfied(cache, &lookup)? {
                continue;
            }

            let _guard = match cache.claim(&lookup.query) {
                ResolveClaim::Run(guard) => guard,

                // Another client is already loading this map;
                // wait for it, then route with the loaded map.
                ResolveClaim::Wait(notify) => {
                    // Register before re-checking the map so a wakeup
                    // between the check and the await isn't missed.
                    let notified = notify.notified();
                    tokio::pin!(notified);
                    notified.as_mut().enable();

                    if !Self::lookup_satisfied(cache, &lookup)? {
                        if safe_timeout(LOOKUP_WAIT_TIMEOUT, notified).await.is_err() {
                            warn!(
                                "sharding key lookup table \"{}\" load wait timed out",
                                lookup.lookup,
                            );
                            return Err(ErrorResponse::sharding_key_lookup(
                                &lookup.lookup,
                                "timed out waiting for an in-flight load",
                            ));
                        }

                        // The loading client failed; it received the
                        // underlying error.
                        if !Self::lookup_satisfied(cache, &lookup)? {
                            return Err(ErrorResponse::sharding_key_lookup(
                                &lookup.lookup,
                                "in-flight load failed",
                            ));
                        }
                    }

                    continue;
                }
            };

            // The map could have loaded while this statement was queued.
            if Self::lookup_satisfied(cache, &lookup)? {
                continue;
            }

            let shard = &shards[round_robin::next() % shards.len()];
            let mut server = shard
                .primary_or_replica(&Request::default())
                .await
                .map_err(|err| {
                    warn!(
                        "sharding key lookup table \"{}\" load couldn't connect: {}",
                        lookup.lookup, err,
                    );
                    ErrorResponse::sharding_key_lookup(
                        &lookup.lookup,
                        &format!("connection failed: {}", err),
                    )
                })?;

            // Re-read the generation and re-run the query if the lookup
            // table was invalidated mid-load: the map may predate the
            // write that caused the invalidation.
            let mut attempts = 0;
            loop {
                attempts += 1;
                let generation = cache.generation(&lookup.lookup);

                // Bounded so a hung shard can't pin the claim: waiters
                // give up after the same timeout, one by one, until the
                // load fails and releases them all. Dropping the server
                // guard mid-query returns the connection to the pool's
                // regular cleanup.
                let rows = safe_timeout(
                    LOOKUP_WAIT_TIMEOUT,
                    server.fetch_all::<DataRow>(lookup.query.as_str()),
                )
                .await
                .map_err(|_| {
                    warn!(
                        "sharding key lookup table \"{}\" load timed out [{}]",
                        lookup.lookup,
                        server.addr(),
                    );
                    ErrorResponse::sharding_key_lookup(&lookup.lookup, "load query timed out")
                })?
                .map_err(|err| {
                    warn!(
                        "sharding key lookup table \"{}\" load failed [{}]: {}",
                        lookup.lookup,
                        server.addr(),
                        err,
                    );
                    ErrorResponse::sharding_key_lookup(
                        &lookup.lookup,
                        &format!("load query failed: {}", err),
                    )
                })?;

                let mut entries = HashMap::with_capacity(rows.len());
                for row in rows {
                    let (Some(value), Some(translated)) = (row.get_text(0), row.get_text(1)) else {
                        return Err(ErrorResponse::sharding_key_lookup(
                            &lookup.lookup,
                            "load query must return two non-NULL columns",
                        ));
                    };
                    entries.insert(value, Arc::from(translated.as_str()));
                }

                if cache.insert_map(&lookup.query, &lookup.lookup, entries, generation) {
                    break;
                }

                if attempts >= LOOKUP_LOAD_ATTEMPTS {
                    return Err(ErrorResponse::sharding_key_lookup(
                        &lookup.lookup,
                        "lookup table invalidated repeatedly during load",
                    ));
                }
            }

            // The map is fresh: the value translates, or the lookup
            // table authoritatively has no row for it.
            if !Self::lookup_satisfied(cache, &lookup)? {
                return Err(ErrorResponse::sharding_key_lookup(
                    &lookup.lookup,
                    "lookup table invalidated during load",
                ));
            }
        }

        Ok(())
    }

    /// The pending lookup is satisfied by the in-memory map: the map is
    /// loaded and, if the lookup carries a value, the value translates.
    ///
    /// A value missing from a fresh map is an error — the lookup table
    /// must have a row for every value that routes through it.
    // The error is an ErrorResponse because it goes to the client as-is.
    #[allow(clippy::result_large_err)]
    fn lookup_satisfied(
        cache: &LookupCache,
        lookup: &PendingLookup,
    ) -> Result<bool, ErrorResponse> {
        let Some(value) = lookup.value.as_deref() else {
            return Ok(cache.loaded(&lookup.query));
        };

        match cache.lookup(&lookup.query, value) {
            MapLookup::Hit(_) => Ok(true),
            MapLookup::NotLoaded | MapLookup::Stale => Ok(false),
            MapLookup::Missing => Err(ErrorResponse::sharding_key_lookup(
                &lookup.lookup,
                &format!("lookup table has no row for value \"{}\"", value),
            )),
        }
    }

    // Make sure we don't send an omni write to a direct-to-shard route.
    // This will cause omni data inconsistency.
    fn is_omnishard_unsafe(backend: &Connection, command: &Command, cluster: &Cluster) -> bool {
        command.route().is_omnisharded()
            && command.route().is_write()
            && backend.connected() // FIXME(lev): I wish there was a way to say >0 and <n in one shot.
            && backend.connected_servers() < cluster.shards().len()
    }

    // Caller switched shards mid-transaction and the transaction is pinned
    // to one shard only.
    fn is_shard_switch(command: &Command, backend: &Connection) -> bool {
        if let Shard::Direct(shard) = command.route().shard() {
            // Round robin doesn't matter, any shard
            // can answer that query.
            if command
                .route()
                .shard_with_priority()
                .source()
                .is_round_robin()
            {
                return false;
            }
            // Session mode shouldn't trigger any checks,
            // you're on your own here.
            if backend.session_mode() {
                return false;
            }
            if let Some(connected_shard) = backend.direct_shard_number()
                && *shard != connected_shard
            {
                return true;
            }
        } else if let Command::Query(route) = command {
            // Tried to run a cross-shard query while connected to one shard only.
            if route.is_cross_shard() && backend.direct_shard_number().is_some() {
                return true;
            }
        }

        false
    }
}
