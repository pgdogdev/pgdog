//! Canonical type OID mappings.
//!
//! Types created with `CREATE TYPE` (or by extensions) get a different OID
//! on each shard. Clients cache type information by OID, so PgDog presents
//! shard 0's OIDs to clients and translates them on the way to and from
//! the other shards.
//!
//! Types created after the mappings were loaded are detected the first time
//! they appear in a message, and the mappings are refreshed from the shards
//! before the message is forwarded.

use super::{Request, Shard, ShardInner};
use crate::{
    backend::{Error, Server},
    net::DataRow,
    sync::SetOnceCell,
};
use parking_lot::{RwLock, RwLockReadGuard};
use std::collections::{HashMap, HashSet};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, OnceLock, Weak};
use tokio::sync::Mutex;
use tracing::{info, warn};

/// OIDs below this are assigned by Postgres at bootstrap/initdb
/// and are assumed to be identical across shards.
pub(crate) const FIRST_USER_OID: u32 = 10000;

#[derive(Debug)]
/// The mapping from a shards type OID to a canonical one
pub(crate) struct Oids {
    canonical_oids: Arc<CanonicalOids>,
    mappings: SetOnceCell<RwLock<OidMappings>>,
    /// The shard these mappings belong to, used to refresh them.
    shard: OnceLock<Weak<ShardInner>>,
    /// Serialize refreshes.
    refresh_lock: Mutex<()>,
    /// A type couldn't be resolved and a full reload was requested.
    stale: AtomicBool,
}

impl Oids {
    pub(crate) fn new(canonical_oids: &Arc<CanonicalOids>) -> Arc<Self> {
        Arc::new(Self {
            canonical_oids: Arc::clone(canonical_oids),
            mappings: Default::default(),
            shard: OnceLock::new(),
            refresh_lock: Mutex::new(()),
            stale: AtomicBool::new(false),
        })
    }

    pub(crate) async fn load(&self, shard: &Shard) -> Result<(), Error> {
        let _ = self.shard.set(Arc::downgrade(&shard.inner));

        self.mappings
            .get_or_try_init(|| async { Ok(RwLock::new(self.fetch(shard).await?)) })
            .await
            .map(|_| ())
    }

    /// Fetch the shard's types and build the mappings against the canonical set.
    async fn fetch(&self, shard: &Shard) -> Result<OidMappings, Error> {
        let mut server = shard.primary_or_replica(&Request::default()).await?;
        let oids = load_oids(&mut server).await?;
        let server_addr = server.addr().clone();
        drop(server);

        let canonical = self.canonical_oids.oids.wait().await.read();
        let mappings = OidMappings::build(oids, &canonical, |type_name| {
            warn!(
                "type {} on shard {} [{}] doesn't exist on shard 0, its OID won't be canonicalized",
                type_name,
                shard.number(),
                server_addr,
            )
        });

        info!(
            "loaded type info for {} types on shard {} [{}]",
            mappings.canonical_to_shard.len(),
            shard.number(),
            server_addr,
        );

        Ok(mappings)
    }

    /// Reload the canonical set and this shard's types.
    ///
    /// Called when a type unknown to the mappings shows up, e.g. after
    /// `CREATE TYPE` ran on another PgDog or directly on the database.
    /// `still_unknown` is re-checked under the refresh lock, so concurrent
    /// detections of the same type result in a single refresh.
    pub(crate) async fn refresh(
        &self,
        still_unknown: impl Fn(&OidMappings) -> bool,
    ) -> Result<(), Error> {
        let _lock = self.refresh_lock.lock().await;

        let mappings = self.mappings.get().ok_or(Error::TypeInfoUnavailable)?;
        if !still_unknown(&mappings.read()) {
            return Ok(());
        }

        let shard = self
            .shard
            .get()
            .and_then(Weak::upgrade)
            .map(|inner| Shard { inner })
            .ok_or(Error::TypeInfoUnavailable)?;

        self.canonical_oids.refresh().await?;
        let refreshed = self.fetch(&shard).await?;
        *mappings.write() = refreshed;

        Ok(())
    }

    pub(crate) async fn wait(&self) {
        self.mappings.wait().await;
    }

    /// Sets this to an empty mapping if it has not already been loaded
    pub(crate) fn skip_load(&self) {
        let _ = self.mappings.set(Default::default());
    }

    /// Get the mappings. Returns `None` if no mappings have been loaded
    pub(crate) fn get(&self) -> Option<RwLockReadGuard<'_, OidMappings>> {
        self.mappings.get().map(RwLock::read)
    }

    /// Stop trying to resolve these OIDs; they have no counterpart on the other side.
    pub(crate) fn mark_unresolvable(&self, oids: impl IntoIterator<Item = u32>) {
        if let Some(mappings) = self.mappings.get() {
            mappings.write().unresolvable.extend(oids);
        }
    }

    /// Record that a type couldn't be resolved, requesting a full reload.
    ///
    /// Returns `true` only the first time, so a single reload is requested.
    pub(crate) fn mark_stale(&self) -> bool {
        !self.stale.swap(true, Ordering::AcqRel)
    }

    #[cfg(test)]
    pub(crate) fn from_canonical(canonical_to_shard: HashMap<u32, u32>) -> Arc<Self> {
        let this = Self {
            canonical_oids: Default::default(),
            mappings: SetOnceCell::from(RwLock::new(OidMappings::default())),
            shard: OnceLock::new(),
            refresh_lock: Mutex::new(()),
            stale: AtomicBool::new(false),
        };
        this.set_canonical(canonical_to_shard);
        Arc::new(this)
    }

    /// Replace the mappings, simulating a refresh.
    #[cfg(test)]
    pub(crate) fn set_canonical(&self, canonical_to_shard: HashMap<u32, u32>) {
        let shard_to_canonical: HashMap<_, _> =
            canonical_to_shard.iter().map(|(&k, &v)| (v, k)).collect();
        let mappings = OidMappings {
            known: shard_to_canonical.keys().copied().collect(),
            canonical_to_shard,
            shard_to_canonical,
            loaded: true,
            ..Default::default()
        };
        *self.mappings.get().expect("test mappings are set").write() = mappings;
    }
}

impl Default for Oids {
    fn default() -> Self {
        Self {
            canonical_oids: Default::default(),
            mappings: SetOnceCell::from(RwLock::new(OidMappings::default())),
            shard: OnceLock::new(),
            refresh_lock: Mutex::new(()),
            stale: AtomicBool::new(false),
        }
    }
}

#[derive(Debug, Default)]
pub(crate) struct OidMappings {
    pub(crate) canonical_to_shard: HashMap<u32, u32>,
    pub(crate) shard_to_canonical: HashMap<u32, u32>,
    /// Every user-defined type OID present on the shard when the mappings were loaded.
    known: HashSet<u32>,
    /// OIDs we tried to resolve and couldn't, e.g. the type exists on one shard only.
    unresolvable: HashSet<u32>,
    /// Mappings were actually loaded from the shard, as opposed to skipped.
    loaded: bool,
}

impl OidMappings {
    /// Build the mappings from the shard's types and the canonical set.
    ///
    /// Types that don't exist on the canonical shard are reported to `missing`
    /// and left unmapped, instead of failing the whole shard.
    fn build(
        oids: impl Iterator<Item = (String, u32)>,
        canonical: &HashMap<String, u32>,
        mut missing: impl FnMut(&str),
    ) -> Self {
        let mut mappings = Self {
            loaded: true,
            ..Default::default()
        };

        for (type_name, oid) in oids {
            mappings.known.insert(oid);
            let Some(&canonical) = canonical.get(&type_name) else {
                missing(&type_name);
                continue;
            };
            if canonical == oid {
                continue;
            }
            mappings.canonical_to_shard.insert(canonical, oid);
            mappings.shard_to_canonical.insert(oid, canonical);
        }

        debug_assert_eq!(
            mappings.canonical_to_shard.len(),
            mappings.shard_to_canonical.len()
        );

        mappings
    }

    /// The mappings were loaded from the shard and can be used
    /// to detect types created since.
    pub(crate) fn loaded(&self) -> bool {
        self.loaded
    }

    fn candidate(&self, oid: u32) -> bool {
        self.loaded && oid >= FIRST_USER_OID && !self.unresolvable.contains(&oid)
    }

    /// An OID sent by the shard (e.g. in RowDescription) that belongs to a type
    /// which didn't exist on the shard when the mappings were loaded.
    pub(crate) fn is_unknown_shard_oid(&self, oid: u32) -> bool {
        self.candidate(oid) && !self.known.contains(&oid)
    }

    /// An OID sent by the client (e.g. in Parse) that neither maps to a type
    /// on this shard nor exists on it unchanged.
    pub(crate) fn is_unknown_canonical_oid(&self, oid: u32) -> bool {
        self.candidate(oid)
            && !self.canonical_to_shard.contains_key(&oid)
            && !self.known.contains(&oid)
    }
}

#[derive(Debug, Default)]
pub(crate) struct CanonicalOids {
    oids: SetOnceCell<RwLock<HashMap<String, u32>>>,
    /// The canonical shard, used to refresh the set.
    shard: OnceLock<Weak<ShardInner>>,
}

impl CanonicalOids {
    pub(crate) async fn load(&self, shard: &Shard) -> Result<(), Error> {
        let _ = self.shard.set(Arc::downgrade(&shard.inner));

        self.oids
            .get_or_try_init(|| async { Ok(RwLock::new(Self::fetch(shard).await?)) })
            .await
            .map(|_| ())
    }

    async fn fetch(shard: &Shard) -> Result<HashMap<String, u32>, Error> {
        let mut server = shard.primary_or_replica(&Request::default()).await?;
        Ok(load_oids(&mut server).await?.collect())
    }

    /// Reload the canonical set from the canonical shard.
    async fn refresh(&self) -> Result<(), Error> {
        let oids = self.oids.get().ok_or(Error::TypeInfoUnavailable)?;
        let shard = self
            .shard
            .get()
            .and_then(Weak::upgrade)
            .map(|inner| Shard { inner })
            .ok_or(Error::TypeInfoUnavailable)?;

        *oids.write() = Self::fetch(&shard).await?;

        Ok(())
    }
}

async fn load_oids(
    server: &mut Server,
) -> Result<impl Iterator<Item = (String, u32)> + use<>, Error> {
    Ok(server
        .fetch_all::<DataRow>(&format!(
            "SELECT nspname || '.' || typname, pg_type.oid FROM pg_type INNER JOIN pg_namespace ON typnamespace = pg_namespace.oid WHERE pg_type.oid >= {FIRST_USER_OID}",
        ))
        .await?
        .into_iter()
        .map(|row| {
            (
                row.get_text(0).expect("selected 2 columns"),
                row.get_int(1, true).expect("selected 2 columns") as u32,
            )
        }))
}

#[cfg(test)]
mod test {
    use super::*;

    fn canonical() -> HashMap<String, u32> {
        [
            ("public.mood", 16400),
            ("public._mood", 16399),
            ("public.same", 16500),
        ]
        .into_iter()
        .map(|(name, oid)| (name.to_owned(), oid))
        .collect()
    }

    fn build(shard: &[(&str, u32)], missing: &mut Vec<String>) -> OidMappings {
        OidMappings::build(
            shard.iter().map(|(name, oid)| (name.to_string(), *oid)),
            &canonical(),
            |name| missing.push(name.to_owned()),
        )
    }

    #[test]
    fn test_build_maps_drifted_types_only() {
        let mut missing = vec![];
        let mappings = build(
            &[
                ("public.mood", 17000),
                ("public._mood", 16999),
                ("public.same", 16500),
            ],
            &mut missing,
        );

        assert!(missing.is_empty());
        assert_eq!(mappings.shard_to_canonical[&17000], 16400);
        assert_eq!(mappings.shard_to_canonical[&16999], 16399);
        assert_eq!(mappings.canonical_to_shard[&16400], 17000);
        assert!(!mappings.shard_to_canonical.contains_key(&16500));
        assert!(mappings.loaded());
    }

    #[test]
    fn test_build_skips_types_missing_on_canonical_shard() {
        let mut missing = vec![];
        let mappings = build(
            &[("public.mood", 17000), ("public.only_here", 18000)],
            &mut missing,
        );

        assert_eq!(missing, vec!["public.only_here"]);
        assert_eq!(mappings.shard_to_canonical.len(), 1);
        // Known to the shard, so it won't trigger a refresh.
        assert!(!mappings.is_unknown_shard_oid(18000));
    }

    #[test]
    fn test_unknown_shard_oid() {
        let mappings = build(
            &[("public.mood", 17000), ("public.same", 16500)],
            &mut vec![],
        );

        assert!(!mappings.is_unknown_shard_oid(17000));
        assert!(!mappings.is_unknown_shard_oid(16500));
        assert!(
            !mappings.is_unknown_shard_oid(25),
            "built-in types are never unknown"
        );
        assert!(mappings.is_unknown_shard_oid(17001));

        assert!(
            !OidMappings::default().is_unknown_shard_oid(17001),
            "skipped mappings can't detect anything"
        );
    }

    #[test]
    fn test_unknown_canonical_oid() {
        let mappings = build(
            &[("public.mood", 17000), ("public.same", 16500)],
            &mut vec![],
        );

        assert!(!mappings.is_unknown_canonical_oid(16400), "mapped");
        assert!(!mappings.is_unknown_canonical_oid(16500), "same on both");
        assert!(!mappings.is_unknown_canonical_oid(25));
        assert!(mappings.is_unknown_canonical_oid(16401));
    }

    #[test]
    fn test_unresolvable() {
        let oids = Oids::from_canonical([(16400, 17000)].into_iter().collect());
        assert!(oids.get().unwrap().is_unknown_shard_oid(18000));
        oids.mark_unresolvable([18000]);
        assert!(!oids.get().unwrap().is_unknown_shard_oid(18000));
        assert!(!oids.get().unwrap().is_unknown_canonical_oid(18000));
    }

    #[test]
    fn test_mark_stale_once() {
        let oids = Oids::from_canonical([(16400, 17000)].into_iter().collect());
        assert!(oids.mark_stale());
        assert!(!oids.mark_stale());
    }
}
