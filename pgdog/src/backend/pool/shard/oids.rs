//! Canonical type OID mappings.
//!
//! Types created with `CREATE TYPE` (or by extensions) get a different OID
//! on each shard. Clients cache type information by OID, so PgDog presents
//! shard 0's OIDs to clients and translates them on the way to and from
//! the other shards.

use super::{Request, Shard};
use crate::{
    backend::{Error, Server},
    net::DataRow,
    sync::SetOnceCell,
};
use std::collections::HashMap;
use std::sync::Arc;
use tracing::info;

mod payload;
pub(crate) use payload::PayloadRewriter;

/// What a type's binary representation looks like, as far as
/// embedded type OIDs are concerned.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum TypeKind {
    /// Array: binary values carry the element type OID.
    Array { element: u32 },
    /// Composite: binary values carry the OID of every field.
    Composite,
    /// Domain: encoded like its base type.
    Domain { base: u32 },
    /// Everything else: no embedded OIDs.
    Other,
}

impl TypeKind {
    /// From `pg_type` columns.
    fn from_catalog(typtype: &str, typcategory: &str, typelem: u32, typbasetype: u32) -> Self {
        match (typtype, typcategory) {
            (_, "A") if typelem != 0 => Self::Array { element: typelem },
            ("c", _) => Self::Composite,
            ("d", _) if typbasetype != 0 => Self::Domain { base: typbasetype },
            _ => Self::Other,
        }
    }
}

/// A type on the shard: `schema.name`, its OID and kind.
type TypeRow = (String, u32, TypeKind);

/// The canonical set of types, from shard 0.
#[derive(Debug, Default)]
pub(crate) struct CanonicalTypes {
    by_name: HashMap<String, u32>,
    kinds: Arc<HashMap<u32, TypeKind>>,
}

impl FromIterator<TypeRow> for CanonicalTypes {
    fn from_iter<I: IntoIterator<Item = TypeRow>>(iter: I) -> Self {
        let mut by_name = HashMap::new();
        let mut kinds = HashMap::new();
        for (name, oid, kind) in iter {
            by_name.insert(name, oid);
            kinds.insert(oid, kind);
        }
        Self {
            by_name,
            kinds: Arc::new(kinds),
        }
    }
}

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
                let mut server = shard.primary_or_replica(&Request::default()).await?;
                let oids = load_oids(&mut server).await?;
                let server_addr = server.addr().clone();
                drop(server);

                let canonical = self.canonical_oids.oids.wait().await;
                let mut canonical_to_shard = HashMap::new();
                let mut shard_to_canonical = HashMap::new();
                let mut shard_kinds = HashMap::new();
                for (type_name, oid, kind) in oids {
                    shard_kinds.insert(oid, kind);
                    let canonical = canonical
                        .by_name
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
                    shard_kinds,
                    canonical_kinds: Arc::clone(&canonical.kinds),
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
        Self::from_canonical_with_kinds(canonical_to_shard, HashMap::new(), HashMap::new())
    }

    /// Mappings plus the type kinds on both sides, simulating what's loaded from `pg_type`.
    #[cfg(test)]
    pub(crate) fn from_canonical_with_kinds(
        canonical_to_shard: HashMap<u32, u32>,
        shard_kinds: HashMap<u32, TypeKind>,
        canonical_kinds: HashMap<u32, TypeKind>,
    ) -> Arc<Self> {
        let shard_to_canonical = canonical_to_shard.iter().map(|(&k, &v)| (v, k)).collect();
        Arc::new(Self {
            canonical_oids: Default::default(),
            mappings: SetOnceCell::from(OidMappings {
                canonical_to_shard,
                shard_to_canonical,
                shard_kinds,
                canonical_kinds: Arc::new(canonical_kinds),
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
    /// Kinds of the shard's types, by shard OID.
    shard_kinds: HashMap<u32, TypeKind>,
    /// Kinds of the canonical types, by canonical OID.
    canonical_kinds: Arc<HashMap<u32, TypeKind>>,
}

impl OidMappings {
    /// Whether canonicalization has anything to do on this shard at all.
    pub(crate) fn is_identity(&self) -> bool {
        self.shard_to_canonical.is_empty()
    }

    /// The shard's OID for a canonical type OID.
    pub(crate) fn shard_oid(&self, canonical: u32) -> u32 {
        self.canonical_to_shard
            .get(&canonical)
            .copied()
            .unwrap_or(canonical)
    }

    /// Rewriter for values coming from the shard (DataRow).
    pub(crate) fn to_canonical(&self) -> PayloadRewriter<'_> {
        PayloadRewriter::new(&self.shard_kinds, &self.shard_to_canonical)
    }

    /// Rewriter for values going to the shard (Bind parameters).
    pub(crate) fn to_shard(&self) -> PayloadRewriter<'_> {
        PayloadRewriter::new(&self.canonical_kinds, &self.canonical_to_shard)
    }
}

#[derive(Debug, Default)]
pub(crate) struct CanonicalOids {
    oids: SetOnceCell<CanonicalTypes>,
}

impl CanonicalOids {
    pub(crate) async fn load(&self, server: &mut Server) -> Result<(), Error> {
        self.oids
            .get_or_try_init(|| async { Ok(load_oids(server).await?.collect()) })
            .await
            .map(|_| ())
    }
}

async fn load_oids(server: &mut Server) -> Result<impl Iterator<Item = TypeRow> + use<>, Error> {
    // OIDs < 10,000 are reserved for PG's internal use and are assumed to be stable
    Ok(server
        .fetch_all::<DataRow>(
            "SELECT nspname || '.' || typname, pg_type.oid, typtype::text, typcategory::text, typelem, typbasetype \
             FROM pg_type INNER JOIN pg_namespace ON typnamespace = pg_namespace.oid \
             WHERE pg_type.oid >= 10000",
        )
        .await?
        .into_iter()
        .map(|row| {
            let name = row.get_text(0).expect("selected 6 columns");
            let oid = row.get_int(1, true).expect("selected 6 columns") as u32;
            let typtype = row.get_text(2).expect("selected 6 columns");
            let typcategory = row.get_text(3).expect("selected 6 columns");
            let typelem = row.get_int(4, true).expect("selected 6 columns") as u32;
            let typbasetype = row.get_int(5, true).expect("selected 6 columns") as u32;
            (
                name,
                oid,
                TypeKind::from_catalog(&typtype, &typcategory, typelem, typbasetype),
            )
        }))
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn test_type_kind_from_catalog() {
        assert_eq!(
            TypeKind::from_catalog("b", "A", 16400, 0),
            TypeKind::Array { element: 16400 }
        );
        assert_eq!(TypeKind::from_catalog("c", "C", 0, 0), TypeKind::Composite);
        assert_eq!(
            TypeKind::from_catalog("d", "N", 0, 23),
            TypeKind::Domain { base: 23 }
        );
        assert_eq!(TypeKind::from_catalog("e", "E", 0, 0), TypeKind::Other);
        // A domain over an array is category A but not itself an array.
        assert_eq!(
            TypeKind::from_catalog("d", "A", 16400, 16399),
            TypeKind::Array { element: 16400 }
        );
    }
}
