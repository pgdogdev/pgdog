//! Upgrade from [`Binding::Direct`] to [`Binding::MultiShard`].
use std::{collections::BTreeSet, mem};

use itertools::Either;

use super::super::*;
use super::MultiShard;

/// Handle adding one or more connections to the binding,
/// as needed by a direct-to-shard or a cross-shard query.
pub(crate) struct MultiShardUpgrade<'a> {
    connection: &'a mut Connection,
}

struct MissingShards {
    missing: Vec<usize>,
    total_shards: usize,
}

impl<'a> MultiShardUpgrade<'a> {
    /// Create new cross-shard connection upgrade handler.
    pub(crate) fn new(connection: &'a mut Connection) -> Self {
        Self { connection }
    }

    /// Change the binding by connecting to required shards to serve the request.
    pub(crate) async fn upgrade(&mut self, request: &Request, route: &Route) -> Result<(), Error> {
        // Don't switch to a replica or to a primary
        // between shard connection upgrades.
        let role_target = match &self.connection.binding {
            Binding::Direct(server) => server.role_target,
            Binding::MultiShard(servers) => servers.role_target,
            _ => return Ok(()),
        };
        // The transaction keeps its own role target, even if this statement has another role.
        let request = &Request {
            role_target,
            ..*request
        };

        let MissingShards {
            missing,
            total_shards,
        } = self.missing_shards(route)?;

        if missing.is_empty() {
            return Ok(());
        }

        let servers = self
            .connection
            .cluster
            .get_conns_for_shards(request, &missing)
            .await?;

        debug_assert_eq!(servers.len(), missing.len());

        let mut servers = servers
            .into_iter()
            .zip(missing)
            .map(|(server, shard)| LinkedServer {
                server,
                shard,
                linked: false,
            })
            .collect::<Vec<_>>();

        let mut binding = match mem::take(&mut self.connection.binding) {
            Binding::Direct(server) => {
                // NOTE: the server can be in a partially comitted state, e.g.,
                // implicit transaction. We expect the client to send a final `Sync`
                // in this case.
                servers.push(server.server);

                MultiBinding {
                    servers,
                    state: MultiShard::new(total_shards, route).boxed(),
                    transaction_stmt: server.transaction_stmt,
                    role_target: server.role_target,
                }
            }

            Binding::MultiShard(mut binding) => {
                // NOTE: same note on partial state as above.
                binding.servers.extend(servers);
                binding.state.update(total_shards, route);

                binding
            }

            _ => return Ok(()),
        };

        binding.sort();

        self.connection.binding = Binding::MultiShard(binding);

        Ok(())
    }

    /// Compute shards required to serve the route,
    /// given currently connected binding.
    ///
    /// Runtime: big-O(shards)
    ///
    fn missing_shards(&self, route: &Route) -> Result<MissingShards, Error> {
        let all = 0..self.connection.cluster()?.shards().len();
        let mut existing = match self.connection.binding {
            Binding::Direct(ref shard) => Either::Left(Some(shard.shard).into_iter()),
            Binding::MultiShard(ref servers) => Either::Right(servers.connected_shards()),
            _ => Either::Left(None.into_iter()),
        }
        .collect::<BTreeSet<_>>();

        let required = match route.shard() {
            Shard::Direct(shard) => Either::Left(Some(*shard).into_iter()),
            Shard::Multi(shards) => Either::Right(Either::Left(shards.iter().copied())),
            Shard::All => Either::Right(Either::Right(all)),
        };

        let mut missing = vec![];

        for shard in required {
            if existing.insert(shard) {
                missing.push(shard);
            }
        }

        Ok(MissingShards {
            missing,
            total_shards: existing.len(),
        })
    }
}
