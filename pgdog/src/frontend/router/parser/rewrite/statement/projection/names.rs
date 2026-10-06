use rand::{Rng, rng};
use std::sync::atomic::{AtomicU64, Ordering};

use super::HelperKind;

static COUNTER: AtomicU64 = AtomicU64::new(0);

#[derive(Clone, Copy)]
pub(crate) enum HelperColumnKind {
    Aggregate(HelperKind),
    OrderBy,
    Case,
}

/// Generate a helper alias with eight random digits and a process-wide counter.
pub(crate) fn helper_column_name(kind: HelperColumnKind) -> String {
    let prefix = match kind {
        HelperColumnKind::Aggregate(HelperKind::Count) => "count_col",
        HelperColumnKind::Aggregate(HelperKind::Sum) => "sum_col",
        HelperColumnKind::Aggregate(HelperKind::SumSquares) => "sumsq_col",
        HelperColumnKind::OrderBy => "order_col",
        HelperColumnKind::Case => "order_case",
    };
    let suffix = rng().random_range(0..100_000_000u32); // Basically we don't conflict with some existing column name in the schema.
    let counter = COUNTER.fetch_add(1, Ordering::Relaxed); // Guarantees against collisions in suffix.
    format!("__pgdog_{prefix}_{suffix:08}_{counter}")
}
