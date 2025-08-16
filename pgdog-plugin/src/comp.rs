/// Rust compiler version used to build this library.
pub fn rustc_version() -> &'static str {
    env!("RUSTC_VERSION")
}
