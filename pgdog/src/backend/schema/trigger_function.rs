//! Function that creates table triggers.
static SOURCE: &str = include_str!("trigger_function.sql");

/// Function source.
pub fn source() -> &'static str {
    SOURCE
}
