use super::*;
use crate::{backend::Cluster, config::ConfigAndUsers};

#[tokio::test]
async fn cached_empty_schema_does_not_query_the_server() {
    let cache = SchemaCache::default();
    let cluster = Cluster::new_test(&ConfigAndUsers::default());
    let shard = &cluster.shards()[0];
    cache.cache.insert(
        (shard.identifier().database.clone(), shard.number()),
        Arc::new(Mutex::new(Some(Schema::default()))),
    );

    // The pool is offline: a cache miss would fail instead of returning an empty schema.
    assert_eq!(
        cache.get(shard).await.expect("cached schema"),
        Schema::default()
    );
}

#[test]
fn reload_preserves_unaffected_cache_entries() {
    for changed_shards in [vec![], vec![1], vec![0], vec![0, 1]] {
        let cache = SchemaCache::default();
        for database in ["changed", "unrelated"] {
            for shard in 0..2 {
                cache.cache.insert(
                    (database.to_owned(), shard),
                    Arc::new(Mutex::new(Some(Schema::default()))),
                );
                cache.oids(database, shard);
            }
        }

        let targets = HashMap::from([(
            "changed".to_owned(),
            changed_shards.iter().copied().collect(),
        )]);
        let refreshed = cache.with_invalidated_shards(&targets);
        for database in ["changed", "unrelated"] {
            let canonical_changed = database == "changed" && changed_shards.contains(&0);
            assert_eq!(
                Arc::ptr_eq(
                    &cache.canonical_oids(database),
                    &refreshed.canonical_oids(database)
                ),
                !canonical_changed,
            );
            for shard in 0..2 {
                let changed = database == "changed" && changed_shards.contains(&shard);
                let key = (database.to_owned(), shard);
                let old = cache
                    .cache
                    .get(&key)
                    .expect("old generation remains intact");
                if changed {
                    assert!(!refreshed.cache.contains_key(&key));
                } else {
                    let new = refreshed
                        .cache
                        .get(&key)
                        .expect("untouched schema retained");
                    assert!(Arc::ptr_eq(old.value(), new.value()));
                }
                assert_eq!(
                    Arc::ptr_eq(
                        &cache.oids(database, shard),
                        &refreshed.oids(database, shard)
                    ),
                    !changed && !canonical_changed,
                );
            }
        }
    }
}
