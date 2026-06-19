use once_cell::sync::Lazy;
use parking_lot::Mutex;
use std::collections::HashMap;
use std::path::PathBuf;

/// Static cache to store leaked PEM key bytes.
///
/// Because `jsonwebtoken`'s `DecodingKey` signature binds the lifetime of the returned key
/// to the lifetime of the input slice, we must provide a slice that lives as long as the
/// program ('static).
///
/// To prevent memory leaks during configuration reloads (e.g. on SIGHUP), we cache loaded
/// PEM files. If a file's path and contents have not changed, we reuse the existing static
/// slice. If the contents change on disk, we leak the new contents and update the cache.
static PEM_KEY_CACHE: Lazy<Mutex<HashMap<PathBuf, &'static [u8]>>> =
    Lazy::new(|| Mutex::new(HashMap::new()));

/// Load a PEM file from disk and return a static byte slice.
/// Safely avoids repeated leaking of memory on configuration reloads.
pub fn get_static_pem_bytes(path: &str) -> std::io::Result<&'static [u8]> {
    let path_buf = PathBuf::from(path);
    let current_bytes = std::fs::read(&path_buf)?;

    let mut cache = PEM_KEY_CACHE.lock();

    // Check if we already have the exact same file contents cached
    if let Some(cached_bytes) = cache.get(&path_buf)
        && *cached_bytes == current_bytes.as_slice()
    {
        return Ok(*cached_bytes);
    }

    // Leaking is only done when a new file is loaded or its contents changed on disk
    let static_ref: &'static [u8] = Box::leak(current_bytes.into_boxed_slice());
    cache.insert(path_buf, static_ref);

    Ok(static_ref)
}
