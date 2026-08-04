use super::{Request, Shard};
use crate::{
    backend::{Error, Server},
    net::DataRow,
    sync::SetOnceCell,
};
use futures::{prelude::*, try_join};
use std::collections::HashMap;
use std::sync::Arc;
use tracing::info;

#[derive(Debug)]
/// The mapping from a shards type OID to a canonical one
pub(crate) struct Oids {
    canonical_oids: Arc<CanonicalOids>,
    mappings: SetOnceCell<OidMappings>,
}

impl Oids {
    pub(crate) fn new(canonical_oids: &Arc<CanonicalOids>) -> Arc<Self> {
        Arc::new(Self {
            canonical_oids: Arc::clone(canonical_oids),
            mappings: Default::default(),
        })
    }

    pub(crate) async fn load(&self, shard: &Shard) -> Result<&OidMappings, Error> {
        self.mappings
            .get_or_try_init(|| async {
                let oids = async {
                    let mut server = shard.primary_or_replica(&Request::default()).await?;
                    Ok::<_, Error>((server.addr().clone(), load_oids(&mut server).await?))
                };
                let canonical = self.canonical_oids.oids.wait().map(Ok);
                let ((server_addr, oids), canonical) = try_join!(oids, canonical)?;
                let mut canonical_to_shard = HashMap::new();
                let mut shard_to_canonical = HashMap::new();
                for (type_name, oid) in oids {
                    let canonical = canonical
                        .get(&type_name)
                        .copied()
                        .ok_or(Error::MissingCanonicalOid(type_name))?;
                    if canonical == oid {
                        continue;
                    }
                    canonical_to_shard.insert(canonical, oid);
                    shard_to_canonical.insert(oid, canonical);
                }

                debug_assert_eq!(canonical_to_shard.len(), shard_to_canonical.len());
                info!(
                    "loaded type info for {} types on shard {} [{}]",
                    canonical_to_shard.len(),
                    shard.number(),
                    server_addr,
                );

                Ok(OidMappings {
                    canonical_to_shard,
                    shard_to_canonical,
                })
            })
            .await
    }

    pub(crate) async fn wait(&self) {
        self.mappings.wait().await;
    }

    /// Sets this to an empty mapping if it has not already been loaded
    pub(crate) fn skip_load(&self) {
        let _ = self.mappings.set(Default::default());
    }

    /// Get the mappings. Returns `None` if no mappings have been loaded
    pub(crate) fn get(&self) -> Option<&OidMappings> {
        self.mappings.get()
    }

    #[cfg(test)]
    pub(crate) fn from_canonical(canonical_to_shard: HashMap<u32, u32>) -> Arc<Self> {
        let shard_to_canonical = canonical_to_shard.iter().map(|(&k, &v)| (v, k)).collect();
        Arc::new(Self {
            canonical_oids: Default::default(),
            mappings: SetOnceCell::from(OidMappings {
                canonical_to_shard,
                shard_to_canonical,
            }),
        })
    }
}

impl Default for Oids {
    fn default() -> Self {
        Self {
            canonical_oids: Default::default(),
            mappings: SetOnceCell::from(OidMappings::default()),
        }
    }
}

#[derive(Debug, Default)]
pub(crate) struct OidMappings {
    pub(crate) canonical_to_shard: HashMap<u32, u32>,
    pub(crate) shard_to_canonical: HashMap<u32, u32>,
}

#[derive(Debug, Default)]
pub(crate) struct CanonicalOids {
    oids: SetOnceCell<HashMap<String, u32>>,
}

impl CanonicalOids {
    pub(crate) async fn load(&self, server: &mut Server) -> Result<(), Error> {
        self.oids
            .get_or_try_init(|| async { Ok(load_oids(server).await?.collect()) })
            .await
            .map(|_| ())
    }
}

async fn load_oids(
    server: &mut Server,
) -> Result<impl Iterator<Item = (String, u32)> + use<>, Error> {
    // OIDs < 10,000 are reserved for PG's internal use and are assumed to be stable
    Ok(server
        .fetch_all::<DataRow>(
            "SELECT nspname || '.' || typname, pg_type.oid FROM pg_type INNER JOIN pg_namespace ON typnamespace = pg_namespace.oid WHERE pg_type.oid >= 10000",
        )
        .await?
        .into_iter()
        .map(|row| {
            (
                row.get_text(0).expect("selected 2 columns"),
                row.get_int(1, true).expect("selected 2 columns") as u32,
            )
        }))
}
