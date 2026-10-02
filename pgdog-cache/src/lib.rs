#![forbid(unsafe_code)]

mod cache;
mod queue;

/// The order in which [`Cache::pop`] evicts entries.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub enum CachePolicy {
    /// Evicts the least recently used entry first.
    #[default]
    LeastRecentlyUsed,

    /// Evicts the entry with the fewest uses first,
    /// breaking ties by least recently used.
    LeastFrequentlyUsed,
}

pub use cache::Cache;
