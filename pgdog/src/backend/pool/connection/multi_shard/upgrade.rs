//! Upgrade from [`Binding::Direct`] to [`Binding::MultiShard`].
use std::collections::{BTreeSet, HashSet};

use itertools::Either;

use super::super::*;
use super::MultiShard;

pub(crate) struct MultiShardUpgrade<'a> {
    connection: &'a mut Connection,
}

struct MissingShards {
    missing: Vec<usize>,
    total_shards: usize,
}

impl<'a> MultiShardUpgrade<'a> {
    pub(crate) fn new(connection: &'a mut Connection) -> Self {
        Self { connection }
    }

    pub(crate) async fn upgrade(&mut self, request: &Request, route: &Route) -> Result<(), Error> {
        if !matches!(
            self.connection.binding,
            Binding::Direct(_) | Binding::MultiShard(_)
        ) {
            return Ok(());
        }

        let MissingShards {
            missing,
            total_shards,
        } = self.missing_shards(route)?;

        if missing.is_empty() {
            return Ok(());
        }

        let mut servers = self
            .connection
            .cluster
            .get_conns_for_shards(request, &missing, route.is_read())
            .await?
            .into_iter()
            .zip(missing.into_iter())
            .map(|(server, shard)| LinkedServer {
                server,
                shard,
                linked: false,
            })
            .collect::<Vec<_>>();

        let mut binding = match self.connection.binding {
            Binding::Direct(server) => {
                servers.push(server);

                MultiBinding {
                    servers,
                    state: MultiShard::new(total_shards, route).boxed(),
                }
            }

            Binding::MultiShard(mut binding) => {
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
