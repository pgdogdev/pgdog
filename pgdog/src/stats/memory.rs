use bytes::{Bytes, BytesMut};
use lru::LruCache;
use std::collections::{BTreeMap, HashMap, HashSet, VecDeque};
use std::hash::Hash;

/// Approximate bytes attributable to a value, for metrics.
///
/// Scalars report their inline size, containers report allocated capacity
/// plus the sum over elements: treat results as an upper bound.
pub trait MemoryUsage {
    fn memory_usage(&self) -> usize;
}

macro_rules! impl_memory_usage_static {
    ($tt:tt) => {
        impl MemoryUsage for $tt {
            #[inline(always)]
            fn memory_usage(&self) -> usize {
                std::mem::size_of::<$tt>()
            }
        }
    };
}

impl_memory_usage_static!(isize);
impl_memory_usage_static!(i64);
impl_memory_usage_static!(i32);
impl_memory_usage_static!(i16);
impl_memory_usage_static!(i8);
impl_memory_usage_static!(usize);
impl_memory_usage_static!(u64);
impl_memory_usage_static!(u32);
impl_memory_usage_static!(u16);
impl_memory_usage_static!(u8);
impl_memory_usage_static!(f32);
impl_memory_usage_static!(f64);
impl_memory_usage_static!(());
impl_memory_usage_static!(bool);

impl MemoryUsage for String {
    #[inline(always)]
    fn memory_usage(&self) -> usize {
        self.capacity()
    }
}

impl<V: MemoryUsage> MemoryUsage for VecDeque<V> {
    #[inline(always)]
    fn memory_usage(&self) -> usize {
        self.iter().map(|v| v.memory_usage()).sum::<usize>()
    }
}

impl<V: MemoryUsage> MemoryUsage for Vec<V> {
    #[inline(always)]
    fn memory_usage(&self) -> usize {
        self.iter().map(|v| v.memory_usage()).sum::<usize>()
    }
}

impl<K: MemoryUsage, V: MemoryUsage, S> MemoryUsage for HashMap<K, V, S> {
    #[inline(always)]
    fn memory_usage(&self) -> usize {
        // The table allocates capacity() slots (plus one control byte each),
        // not len(): spare capacity left behind by removed entries still
        // occupies memory and has to be counted.
        self.capacity() * (std::mem::size_of::<(K, V)>() + 1)
            + self
                .iter()
                .map(|(k, v)| k.memory_usage() + v.memory_usage())
                .sum::<usize>()
    }
}

impl<K: MemoryUsage, V: MemoryUsage> MemoryUsage for BTreeMap<K, V> {
    #[inline(always)]
    fn memory_usage(&self) -> usize {
        self.iter()
            .map(|(k, v)| k.memory_usage() + v.memory_usage())
            .sum::<usize>()
    }
}

impl<V: MemoryUsage, S> MemoryUsage for HashSet<V, S> {
    #[inline(always)]
    fn memory_usage(&self) -> usize {
        // Same as HashMap: count allocated slots, not just live entries.
        self.capacity() * (std::mem::size_of::<V>() + 1)
            + self.iter().map(|v| v.memory_usage()).sum::<usize>()
    }
}

impl<K: MemoryUsage + Hash + Eq, V: MemoryUsage + Eq> MemoryUsage for LruCache<K, V> {
    #[inline(always)]
    fn memory_usage(&self) -> usize {
        self.iter()
            .map(|(k, v)| k.memory_usage() + v.memory_usage())
            .sum::<usize>()
    }
}

impl MemoryUsage for BytesMut {
    #[inline(always)]
    fn memory_usage(&self) -> usize {
        self.capacity()
    }
}

impl MemoryUsage for Bytes {
    #[inline(always)]
    fn memory_usage(&self) -> usize {
        0
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn hash_map_counts_spare_capacity() {
        let mut map: HashMap<usize, usize> = HashMap::new();
        for i in 0..1000 {
            map.insert(i, i);
        }
        let capacity = map.capacity();
        for i in 0..1000 {
            map.remove(&i);
        }
        assert!(map.is_empty());
        // The allocation survives removals; capacity() may dip slightly
        // due to tombstones but stays the same order of magnitude.
        assert!(map.capacity() * 2 >= capacity);
        let floor = map.capacity() * (std::mem::size_of::<(usize, usize)>() + 1);
        assert!(
            map.memory_usage() >= floor,
            "spare capacity must be counted"
        );
    }

    #[test]
    fn hash_set_counts_spare_capacity() {
        let mut set: HashSet<usize> = HashSet::new();
        for i in 0..1000 {
            set.insert(i);
        }
        let capacity = set.capacity();
        set.clear();
        assert_eq!(set.capacity(), capacity);
        assert!(set.memory_usage() >= capacity * (std::mem::size_of::<usize>() + 1));
    }
}
