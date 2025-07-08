use bytes::{Bytes, BytesMut};
use lru::LruCache;
use std::collections::{BTreeMap, HashMap, HashSet, VecDeque};
use std::hash::Hash;

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

impl<K: MemoryUsage, V: MemoryUsage> MemoryUsage for HashMap<K, V> {
    #[inline(always)]
    fn memory_usage(&self) -> usize {
        self.iter()
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

impl<V: MemoryUsage> MemoryUsage for HashSet<V> {
    #[inline(always)]
    fn memory_usage(&self) -> usize {
        self.iter().map(|v| v.memory_usage()).sum::<usize>()
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
