use once_cell::sync::Lazy;
use parking_lot::Mutex;
use std::collections::HashMap;
use std::time::{Duration, SystemTime};

use crate::backend::pool::Address;

/// How early to consider a token expired to avoid edge-cases at the boundary.
const EXPIRY_BUFFER: Duration = Duration::from_secs(30);

#[derive(Clone)]
pub(super) struct CachedToken {
    pub(super) token: String,
    expires_at: SystemTime,
}

impl CachedToken {
    pub(super) fn new(token: String, expires_at: SystemTime) -> Self {
        Self { token, expires_at }
    }

    pub(super) fn is_valid(&self) -> bool {
        SystemTime::now()
            .checked_add(EXPIRY_BUFFER)
            .map(|t| t < self.expires_at)
            .unwrap_or(false)
    }
}

#[derive(Clone, PartialEq, Eq, Hash)]
pub(super) struct CacheKey {
    pub(super) user: String,
    pub(super) host: String,
    pub(super) port: u16,
}

impl From<&Address> for CacheKey {
    fn from(addr: &Address) -> Self {
        Self {
            user: addr.user.clone(),
            host: addr.host.clone(),
            port: addr.port,
        }
    }
}

pub(super) static TOKEN_CACHE: Lazy<Mutex<HashMap<CacheKey, CachedToken>>> =
    Lazy::new(|| Mutex::new(HashMap::new()));

pub(super) fn get(key: &CacheKey) -> Option<String> {
    TOKEN_CACHE
        .lock()
        .get(key)
        .filter(|c| c.is_valid())
        .map(|c| c.token.clone())
}

pub(super) fn set(key: CacheKey, token: String, expires_at: SystemTime) {
    TOKEN_CACHE
        .lock()
        .insert(key, CachedToken::new(token, expires_at));
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn valid_before_expiry() {
        let cached = CachedToken::new("tok".into(), SystemTime::now() + Duration::from_secs(120));
        assert!(cached.is_valid());
    }

    #[test]
    fn invalid_within_buffer() {
        let cached = CachedToken::new(
            "tok".into(),
            // Expires in 10 s — inside the 30 s buffer.
            SystemTime::now() + Duration::from_secs(10),
        );
        assert!(!cached.is_valid());
    }

    #[test]
    fn invalid_after_expiry() {
        let cached = CachedToken::new("tok".into(), SystemTime::now() - Duration::from_secs(1));
        assert!(!cached.is_valid());
    }
}
