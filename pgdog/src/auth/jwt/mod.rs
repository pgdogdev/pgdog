pub mod error;
pub mod pem_cache;

#[cfg(test)]
pub mod tests;

use crate::config::General;
use once_cell::sync::Lazy;
use serde::{Deserialize, Serialize};

pub use error::JwtError;

/// Represents the claims contained within the validated JWT token.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Claims {
    /// The subject claim, mapped directly to the target PostgreSQL role name.
    pub sub: String,
    /// Expiration time (seconds since UNIX epoch).
    pub exp: u64,
    /// Optional audience claim used to verify token target.
    pub aud: Option<serde_json::Value>,
    /// All other fields captured dynamically.
    #[serde(flatten)]
    pub extra: std::collections::HashMap<String, serde_json::Value>,
}

impl Claims {
    /// Extract the username dynamically using the configured claim name.
    pub fn get_username(&self, claim_name: &str) -> Option<String> {
        if claim_name == "sub" {
            Some(self.sub.clone())
        } else if let Some(val) = self.extra.get(claim_name) {
            match val {
                serde_json::Value::String(s) => Some(s.clone()),
                _ => Some(val.to_string()),
            }
        } else {
            None
        }
    }
}

struct CachedJwks {
    jwks: std::sync::Arc<jsonwebtoken::jwk::JwkSet>,
    fetched_at: std::time::Instant,
}

static JWT_CRYPTO_PROVIDER: Lazy<()> = Lazy::new(|| {
    let _ = tokio_rustls::rustls::crypto::aws_lc_rs::default_provider().install_default();
});

/// Dynamic JWT validator supporting signature verification using either a local PEM public key
/// or a remote JSON Web Key Set (JWKS) endpoint.
pub struct JwtValidator {
    public_key: Option<jsonwebtoken::DecodingKey>,
    jwks_url: Option<String>,
    jwks_cache_ttl: std::time::Duration,
    audience: Option<String>,
    username_claim: String,
    jwks_cache: tokio::sync::Mutex<Option<CachedJwks>>,
    http_client: reqwest::Client,
}

impl JwtValidator {
    /// Creates a new `JwtValidator` using the settings from the `General` configuration.
    pub fn new(general: &General) -> Result<Self, JwtError> {
        Lazy::force(&JWT_CRYPTO_PROVIDER);

        let public_key = if let Some(ref path) = general.jwt_public_key_file {
            let static_bytes = pem_cache::get_static_pem_bytes(path).map_err(|e| {
                JwtError::PublicKeyLoad(format!("Failed to read public key file: {}", e))
            })?;

            // Try to parse as RSA first, fallback to EC public key
            let key = if let Ok(rsa_key) = jsonwebtoken::DecodingKey::from_rsa_pem(static_bytes) {
                rsa_key
            } else {
                jsonwebtoken::DecodingKey::from_ec_pem(static_bytes).map_err(|e| {
                    JwtError::PublicKeyLoad(format!("Failed to parse RSA/EC public key PEM: {}", e))
                })?
            };
            Some(key)
        } else {
            None
        };

        Ok(Self {
            public_key,
            jwks_url: general.jwt_jwks_url.clone(),
            jwks_cache_ttl: std::time::Duration::from_secs(general.jwt_jwks_cache_ttl),
            audience: general.jwt_audience.clone(),
            username_claim: general.jwt_username_claim.clone(),
            jwks_cache: tokio::sync::Mutex::new(None),
            http_client: reqwest::Client::new(),
        })
    }

    /// Validates the provided JWT token string.
    pub async fn validate(&self, token: &str) -> Result<Claims, JwtError> {
        let header = jsonwebtoken::decode_header(token)?;

        // Ensure we borrow the decoding key to avoid cloning
        let decoding_key_owned;
        let decoding_key = if let Some(ref public_key) = self.public_key {
            public_key
        } else if let Some(ref jwks_url) = self.jwks_url {
            let kid = header.kid.as_ref().ok_or(JwtError::MissingKid)?;
            let jwks = self.get_jwks(jwks_url).await?;

            let jwk = jwks
                .keys
                .iter()
                .find(|k| k.common.key_id.as_ref() == Some(kid))
                .ok_or(JwtError::JwkNotFound)?;

            decoding_key_owned = jsonwebtoken::DecodingKey::from_jwk(jwk)
                .map_err(|e| JwtError::InvalidJwk(e.to_string()))?;
            &decoding_key_owned
        } else {
            return Err(JwtError::PublicKeyLoad(
                "No JWT verification key or JWKS URL configured".to_string(),
            ));
        };

        // Determine algorithm from header, only allowing secure asymmetric algorithms
        let algorithm = match header.alg {
            jsonwebtoken::Algorithm::RS256 => jsonwebtoken::Algorithm::RS256,
            jsonwebtoken::Algorithm::ES256 => jsonwebtoken::Algorithm::ES256,
            _ => return Err(JwtError::InvalidAlgorithm),
        };

        // Configure validation criteria
        let mut validation = jsonwebtoken::Validation::new(algorithm);
        if let Some(ref aud) = self.audience {
            validation.set_audience(&[aud]);
        } else {
            validation.validate_aud = false;
        }

        let token_data = jsonwebtoken::decode::<Claims>(token, decoding_key, &validation)?;
        let claims = token_data.claims;

        let username = claims
            .get_username(&self.username_claim)
            .ok_or(JwtError::MissingSub)?;

        if username.is_empty() {
            return Err(JwtError::MissingSub);
        }

        Ok(claims)
    }

    /// Fetches the JWKS keys using double-checked locking to avoid duplicate HTTP calls.
    async fn get_jwks(
        &self,
        jwks_url: &str,
    ) -> Result<std::sync::Arc<jsonwebtoken::jwk::JwkSet>, JwtError> {
        // First check (read lock)
        {
            let cache = self.jwks_cache.lock().await;
            if let Some(ref cached) = *cache
                && cached.fetched_at.elapsed() < self.jwks_cache_ttl
            {
                return Ok(std::sync::Arc::clone(&cached.jwks));
            }
        }

        // Second check under lock to prevent concurrent HTTP requests
        let mut cache = self.jwks_cache.lock().await;
        if let Some(ref cached) = *cache
            && cached.fetched_at.elapsed() < self.jwks_cache_ttl
        {
            return Ok(std::sync::Arc::clone(&cached.jwks));
        }

        let response = self
            .http_client
            .get(jwks_url)
            .send()
            .await
            .map_err(|e| JwtError::JwksFetch(format!("Request failed: {}", e)))?;

        let jwks = std::sync::Arc::new(
            response
                .json::<jsonwebtoken::jwk::JwkSet>()
                .await
                .map_err(|e| JwtError::JwksFetch(format!("Failed to parse JWKS JSON: {}", e)))?,
        );

        *cache = Some(CachedJwks {
            jwks: std::sync::Arc::clone(&jwks),
            fetched_at: std::time::Instant::now(),
        });

        Ok(jwks)
    }
}
