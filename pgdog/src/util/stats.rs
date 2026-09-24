use std::time::Instant;

/// Average per-second rate of `count` events since `started`.
/// `None` before any measurable time has passed.
pub(crate) fn average_rate(count: u64, started: Instant) -> Option<u64> {
    let elapsed = started.elapsed().as_secs_f64();
    (elapsed > 0.0).then(|| (count as f64 / elapsed) as u64)
}
