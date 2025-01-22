use once_cell::sync::Lazy;
use std::sync::atomic::{AtomicUsize, Ordering};

static ROUND_ROBIN: Lazy<AtomicUsize> = Lazy::new(|| AtomicUsize::new(0));

/// Get next round robin number.
pub fn next() -> usize {
    ROUND_ROBIN.fetch_add(1, Ordering::Relaxed)
}
