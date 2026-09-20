use std::{
    borrow::Borrow,
    fmt::{self, Debug, Formatter},
    hash::{BuildHasher, Hash},
    mem,
};

use hashbrown::{DefaultHashBuilder, HashTable, hash_table::Entry};

use crate::{
    CachePolicy,
    queue::{EvictionQueue, NodeIndex},
};

/// A hash map that can efficiently pop the least recently used or least frequently used entry.
/// Entries are kept in [`CachePolicy`] eviction order, with O(1) lookups, inserts, and removals.
pub struct Cache<K, V> {
    hasher: DefaultHashBuilder,
    table: HashTable<NodeIndex>,
    queue: EvictionQueue<(K, V)>,
}

impl<K, V> Default for Cache<K, V> {
    fn default() -> Self {
        Self {
            hasher: DefaultHashBuilder::default(),
            table: HashTable::default(),
            queue: EvictionQueue::default(),
        }
    }
}

impl<K, V> Debug for Cache<K, V> {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        f.debug_struct("Cache")
            .field("policy", &self.queue.policy())
            .field("len", &self.len())
            .finish_non_exhaustive()
    }
}

impl<K, V> Cache<K, V> {
    /// Creates an empty `Cache`.
    pub fn new() -> Self {
        Self::default()
    }

    /// Creates an empty `Cache` with the specified capacity.
    pub fn with_capacity(capacity: usize) -> Self {
        Self {
            hasher: DefaultHashBuilder::default(),
            table: HashTable::with_capacity(capacity),
            queue: EvictionQueue::with_capacity(capacity),
        }
    }

    /// Sets the eviction policy.
    /// Takes O(n) when switching to LRU and O(1) otherwise.
    /// Does nothing when the policy is unchanged.
    /// Switching to LRU keeps the current eviction order.
    /// Switching to LFU counts every entry as used once.
    pub fn configure(&mut self, policy: CachePolicy) {
        self.queue.configure(policy);
    }

    /// Returns the number of entries in the cache.
    pub fn len(&self) -> usize {
        self.table.len()
    }

    /// Returns `true` if the cache has no entries.
    pub fn is_empty(&self) -> bool {
        self.table.is_empty()
    }

    /// Removes every entry in the cache.
    /// Retains the allocated memory for reuse.
    pub fn clear(&mut self) {
        self.table.clear();
        self.queue.clear();
    }
}

impl<K: Hash + Eq, V> Cache<K, V> {
    /// Inserts `value` for `key` in the cache.
    ///
    /// Returns the previous value if `key` was present.
    pub fn insert(&mut self, key: K, value: V) -> Option<V> {
        let hash = self.hasher.hash_one(&key);

        let entry = self.table.entry(
            hash,
            |&node| self.queue.get(node).0 == key,
            |&node| self.hasher.hash_one(&self.queue.get(node).0),
        );

        match entry {
            Entry::Occupied(entry) => {
                let node = *entry.get();

                self.queue.promote(node);

                let item = self.queue.get_mut(node);

                Some(mem::replace(&mut item.1, value))
            }

            Entry::Vacant(entry) => {
                entry.insert(self.queue.push((key, value)));

                None
            }
        }
    }

    /// Returns the value for `key`.
    pub fn get<Q>(&mut self, key: &Q) -> Option<&V>
    where
        K: Borrow<Q>,
        Q: Hash + Eq + ?Sized,
    {
        self.find(key).map(|node| {
            self.queue.promote(node);
            &self.queue.get(node).1
        })
    }

    /// Returns a mutable reference to the value for `key`.
    pub fn get_mut<Q>(&mut self, key: &Q) -> Option<&mut V>
    where
        K: Borrow<Q>,
        Q: Hash + Eq + ?Sized,
    {
        self.find(key).map(|node| {
            self.queue.promote(node);
            &mut self.queue.get_mut(node).1
        })
    }

    /// Returns the value for `key` without recording a **use**.
    pub fn peek<Q>(&self, key: &Q) -> Option<&V>
    where
        K: Borrow<Q>,
        Q: Hash + Eq + ?Sized,
    {
        self.find(key).map(|node| &self.queue.get(node).1)
    }

    /// Returns a mutable reference to the value for `key` without recording a **use**.
    pub fn peek_mut<Q>(&mut self, key: &Q) -> Option<&mut V>
    where
        K: Borrow<Q>,
        Q: Hash + Eq + ?Sized,
    {
        self.find(key).map(|node| &mut self.queue.get_mut(node).1)
    }

    /// Removes `key`, returning its value if it was present.
    pub fn remove<Q>(&mut self, key: &Q) -> Option<V>
    where
        K: Borrow<Q>,
        Q: Hash + Eq + ?Sized,
    {
        let hash = self.hasher.hash_one(key);

        let (node, _) = self
            .table
            .find_entry(hash, |&node| self.queue.get(node).0.borrow() == key)
            .ok()?
            .remove();

        Some(self.queue.remove(node).1)
    }

    /// Removes and returns the next entry in [`CachePolicy`] eviction order.
    pub fn pop(&mut self) -> Option<(K, V)> {
        let node = self.queue.front()?;
        let hash = self.hasher.hash_one(&self.queue.get(node).0);

        // The node is known so match by its index to skip the key comparison.
        self.table
            .find_entry(hash, |&index| index == node)
            .expect("queued node should be in the table")
            .remove();

        Some(self.queue.remove(node))
    }

    /// Returns the node holding `key`.
    #[inline]
    fn find<Q>(&self, key: &Q) -> Option<NodeIndex>
    where
        K: Borrow<Q>,
        Q: Hash + Eq + ?Sized,
    {
        let hash = self.hasher.hash_one(key);

        self.table
            .find(hash, |&node| self.queue.get(node).0.borrow() == key)
            .copied()
    }
}

#[cfg(test)]
mod tests {
    use std::iter::from_fn;

    use super::*;

    fn drain<K: Hash + Eq, V>(cache: &mut Cache<K, V>) -> Vec<(K, V)> {
        from_fn(|| cache.pop()).collect()
    }

    #[test]
    fn insert_replaces_an_existing_value() {
        let mut cache = Cache::default();

        assert_eq!(cache.insert(1, 1), None);
        assert_eq!(cache.insert(1, 2), Some(1));
        assert_eq!(cache.len(), 1);
        assert_eq!(cache.peek(&1), Some(&2));
    }

    #[test]
    fn insert_of_an_existing_key_records_a_use() {
        let mut cache = Cache::default();

        cache.insert(1, 1);
        cache.insert(2, 2);
        cache.insert(1, 1);

        assert_eq!(drain(&mut cache), [(2, 2), (1, 1)]);
    }

    #[test]
    fn get_and_get_mut_record_a_use() {
        let mut cache = Cache::default();

        cache.insert(1, 1);
        cache.insert(2, 2);
        cache.insert(3, 3);

        assert_eq!(cache.get(&1), Some(&1));
        assert_eq!(cache.get_mut(&2), Some(&mut 2));

        assert_eq!(drain(&mut cache), [(3, 3), (1, 1), (2, 2)]);
    }

    #[test]
    fn peek_and_peek_mut_do_not_record_a_use() {
        let mut cache = Cache::default();

        cache.insert(1, 1);
        cache.insert(2, 2);

        assert_eq!(cache.peek(&1), Some(&1));
        assert_eq!(cache.peek_mut(&1), Some(&mut 1));

        assert_eq!(drain(&mut cache), [(1, 1), (2, 2)]);
    }

    #[test]
    fn remove_takes_the_key_out_of_eviction_order() {
        let mut cache = Cache::default();

        cache.insert(1, 1);
        cache.insert(2, 2);
        cache.insert(3, 3);

        assert_eq!(cache.remove(&2), Some(2));
        assert_eq!(cache.remove(&2), None);
        assert_eq!(drain(&mut cache), [(1, 1), (3, 3)]);
    }

    #[test]
    fn pop_removes_the_key() {
        let mut cache = Cache::default();

        cache.insert(1, 1);
        cache.insert(2, 2);

        assert_eq!(cache.pop(), Some((1, 1)));
        assert_eq!(cache.get(&1), None);
        assert_eq!(cache.len(), 1);
    }

    #[test]
    fn clear_empties_the_cache() {
        let mut cache = Cache::default();

        cache.insert(1, 1);
        cache.insert(2, 2);
        cache.clear();

        assert!(cache.is_empty());
        assert_eq!(cache.pop(), None);
    }
}
