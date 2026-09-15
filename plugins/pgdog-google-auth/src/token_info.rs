use std::{
    collections::HashSet,
    io::{Read, Take},
};

use pgdog_plugin::AuthGrant;
use serde::Deserialize;
use thiserror::Error;

use crate::config::{RuntimeConfig, Settings, UsernameClaim};

const MAX_TOKEN_LENGTH: usize = 16 * 1024;
const MAX_RESPONSE_LENGTH: u64 = 64 * 1024;
const MAX_POSTGRES_USERNAME_LENGTH: usize = 63;

#[derive(Debug, Deserialize)]
struct TokenInfo {
    #[serde(alias = "aud")]
    audience: Option<String>,
    #[serde(alias = "azp")]
    issued_to: Option<String>,
    #[serde(alias = "sub")]
    user_id: Option<String>,
    scope: Option<String>,
    #[serde(default, deserialize_with = "deserialize_str_i64")]
    expires_in: Option<i64>,
    email: Option<String>,
    #[serde(
        default,
        alias = "email_verified",
        deserialize_with = "deserialize_str_bool"
    )]
    verified_email: Option<bool>,
}

/// Google's tokeninfo endpoint stringifies booleans (`"email_verified":
/// "true"`), while other identity endpoints use real JSON booleans; accept
/// both. `deserialize_with` disables serde's implicit missing-field handling
/// for `Option`, hence the explicit `default` on the field above.
fn deserialize_str_bool<'de, D>(deserializer: D) -> Result<Option<bool>, D::Error>
where
    D: serde::de::Deserializer<'de>,
{
    struct StrBool;

    impl serde::de::Visitor<'_> for StrBool {
        type Value = bool;

        fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            formatter.write_str(r#"a boolean or "true"/"false""#)
        }

        fn visit_bool<E>(self, value: bool) -> Result<Self::Value, E>
        where
            E: serde::de::Error,
        {
            Ok(value)
        }

        fn visit_str<E>(self, value: &str) -> Result<Self::Value, E>
        where
            E: serde::de::Error,
        {
            match value {
                "true" => Ok(true),
                "false" => Ok(false),
                _ => Err(E::unknown_variant(value, &["true", "false"])),
            }
        }
    }

    deserializer.deserialize_any(StrBool).map(Some)
}

/// Seconds are a JSON number on some endpoints and a decimal string on
/// others (Google's tokeninfo sends `"expires_in": "3599"`); accept both, and
/// reject anything else rather than panicking on a bad parse. As with
/// [`deserialize_str_bool`], `deserialize_with` disables serde's implicit
/// handling of a missing `Option` field, hence the `default` on the field.
fn deserialize_str_i64<'de, D>(deserializer: D) -> Result<Option<i64>, D::Error>
where
    D: serde::de::Deserializer<'de>,
{
    struct StrI64;

    impl serde::de::Visitor<'_> for StrI64 {
        type Value = i64;

        fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            formatter.write_str("an integer or a decimal string")
        }

        fn visit_i64<E>(self, value: i64) -> Result<Self::Value, E>
        where
            E: serde::de::Error,
        {
            Ok(value)
        }

        fn visit_u64<E>(self, value: u64) -> Result<Self::Value, E>
        where
            E: serde::de::Error,
        {
            i64::try_from(value).map_err(|_| E::custom("value is out of range"))
        }

        fn visit_str<E>(self, value: &str) -> Result<Self::Value, E>
        where
            E: serde::de::Error,
        {
            value
                .trim()
                .parse()
                .map_err(|_| E::custom("expected a decimal string"))
        }
    }

    deserializer.deserialize_any(StrI64).map(Some)
}

pub(crate) fn authenticate(
    runtime: &RuntimeConfig,
    startup_user: &str,
    credential: &str,
    tls: bool,
) -> Result<AuthGrant, AuthenticationError> {
    if runtime.settings.require_tls && !tls {
        return Err(AuthenticationError::TlsRequired);
    }
    if credential.is_empty() || credential.len() > MAX_TOKEN_LENGTH {
        return Err(AuthenticationError::InvalidCredential);
    }

    let token_info = fetch(runtime, credential)?;
    validate(&runtime.settings, startup_user, token_info).map(|username| AuthGrant {
        derived_user: Some(username.clone()),
        server_role: runtime.settings.impersonate.then_some(username),
        server_user: runtime.settings.server_user.clone(),
        server_password: runtime.server_password.clone(),
        read_only: runtime.settings.read_only,
        provision: runtime.settings.provision,
    })
}

fn fetch(runtime: &RuntimeConfig, credential: &str) -> Result<TokenInfo, AuthenticationError> {
    // POST the token in the request body. A query string ends up in access
    // logs, proxy logs and browser-style history along the way; Google accepts
    // both spellings.
    let response = runtime
        .client
        .post(runtime.endpoint.clone())
        .form(&[("access_token", credential)])
        .send()
        .map_err(request_error)?;

    if !response.status().is_success() {
        return Err(AuthenticationError::Rejected);
    }

    let mut body = Vec::new();
    let mut limited: Take<_> = response.take(MAX_RESPONSE_LENGTH + 1);
    limited
        .read_to_end(&mut body)
        .map_err(|error| AuthenticationError::Request(error.to_string()))?;
    if body.len() as u64 > MAX_RESPONSE_LENGTH {
        return Err(AuthenticationError::ResponseTooLarge);
    }

    serde_json::from_slice(&body).map_err(AuthenticationError::InvalidResponse)
}

fn request_error(error: reqwest::Error) -> AuthenticationError {
    AuthenticationError::Request(error.without_url().to_string())
}

fn validate(
    settings: &Settings,
    startup_user: &str,
    token_info: TokenInfo,
) -> Result<String, AuthenticationError> {
    // A token that reports no remaining lifetime, or no lifetime at all, is
    // not accepted.
    if token_info.expires_in.unwrap_or_default() <= 0 {
        return Err(AuthenticationError::Expired);
    }

    validate_audience(settings, &token_info)?;
    validate_scopes(settings, &token_info)?;

    let email = token_info
        .email
        .as_deref()
        .map(str::trim)
        .filter(|email| !email.is_empty())
        .map(str::to_ascii_lowercase);
    let email_required = settings.username_claim == UsernameClaim::Email
        || !settings.allowed_domains.is_empty()
        || !settings.allowed_emails.is_empty();

    if email_required && email.is_none() {
        return Err(AuthenticationError::MissingEmail);
    }
    if email_required && settings.require_verified_email && token_info.verified_email != Some(true)
    {
        return Err(AuthenticationError::UnverifiedEmail);
    }
    if email_required {
        validate_email(email.as_deref().ok_or(AuthenticationError::MissingEmail)?)?;
    }

    if !settings.allowed_domains.is_empty() || !settings.allowed_emails.is_empty() {
        let email = email.as_deref().ok_or(AuthenticationError::MissingEmail)?;
        let (_, domain) = validate_email(email)?;
        let allowed = settings
            .allowed_emails
            .iter()
            .any(|allowed| allowed == email)
            || settings
                .allowed_domains
                .iter()
                .any(|allowed| allowed == domain);
        if !allowed {
            return Err(AuthenticationError::PrincipalNotAllowed);
        }
    }

    let username = match settings.username_claim {
        UsernameClaim::Email => {
            let email = email.ok_or(AuthenticationError::MissingEmail)?;
            if settings.strip_email_domain {
                validate_email(&email)?.0.to_owned()
            } else {
                email
            }
        }
        UsernameClaim::UserId => token_info
            .user_id
            .as_deref()
            .map(str::trim)
            .filter(|user_id| !user_id.is_empty())
            .map(str::to_owned)
            .ok_or(AuthenticationError::MissingUserId)?,
    };

    if username.len() > MAX_POSTGRES_USERNAME_LENGTH {
        return Err(AuthenticationError::UsernameTooLong);
    }
    // The name becomes a PostgreSQL role, a `server_role` startup parameter
    // and a PgDog config entry. Only reachable from a hostile `tokeninfo_url`,
    // since Google does not mint identities like this, but cheap to refuse.
    if username
        .chars()
        .any(|c| c.is_control() || c.is_whitespace())
    {
        return Err(AuthenticationError::InvalidUsername);
    }
    // `claims_user` decides whether to handle this login by lower-casing the
    // startup user, so compare the same way: otherwise `Alice@Example.com` is
    // claimed and then denied instead of being left to the next plugin.
    if settings.require_user_match && !startup_user.eq_ignore_ascii_case(&username) {
        return Err(AuthenticationError::UserMismatch);
    }

    Ok(username)
}

fn validate_email(email: &str) -> Result<(&str, &str), AuthenticationError> {
    let (local, domain) = email
        .rsplit_once('@')
        .ok_or(AuthenticationError::InvalidEmail)?;

    if local.is_empty() || domain.is_empty() {
        return Err(AuthenticationError::InvalidEmail);
    }

    Ok((local, domain))
}

fn validate_audience(
    settings: &Settings,
    token_info: &TokenInfo,
) -> Result<(), AuthenticationError> {
    // `RuntimeConfig::from_settings` refuses to build without an audience
    // allow-list; if an empty one ever reaches here, accept nothing.
    let accepted = !settings.allowed_audiences.is_empty()
        && token_info
            .audience
            .iter()
            .chain(token_info.issued_to.iter())
            .any(|audience| {
                settings
                    .allowed_audiences
                    .iter()
                    .any(|allowed| allowed == audience)
            });

    accepted
        .then_some(())
        .ok_or(AuthenticationError::AudienceNotAllowed)
}

fn validate_scopes(settings: &Settings, token_info: &TokenInfo) -> Result<(), AuthenticationError> {
    if settings.required_scopes.is_empty() {
        return Ok(());
    }

    let scopes: HashSet<_> = token_info
        .scope
        .as_deref()
        .unwrap_or_default()
        .split_whitespace()
        .collect();

    settings
        .required_scopes
        .iter()
        .all(|scope| scopes.contains(scope.as_str()))
        .then_some(())
        .ok_or(AuthenticationError::MissingScope)
}

#[derive(Debug, Error)]
pub(crate) enum AuthenticationError {
    #[error("Google access tokens require a TLS client connection")]
    TlsRequired,
    #[error("invalid Google access token")]
    InvalidCredential,
    #[error("Google tokeninfo request failed: {0}")]
    Request(String),
    #[error("Google rejected the access token")]
    Rejected,
    #[error("Google tokeninfo response exceeded the size limit")]
    ResponseTooLarge,
    #[error("Google tokeninfo returned an invalid response: {0}")]
    InvalidResponse(#[source] serde_json::Error),
    #[error("Google access token is expired")]
    Expired,
    #[error("Google access token has no email identity")]
    MissingEmail,
    #[error("Google access token email is not verified")]
    UnverifiedEmail,
    #[error("Google access token contains an invalid email identity")]
    InvalidEmail,
    #[error("Google account is not allowed")]
    PrincipalNotAllowed,
    #[error("Google access token audience is not allowed")]
    AudienceNotAllowed,
    #[error("Google access token is missing a required scope")]
    MissingScope,
    #[error("Google access token has no user_id identity")]
    MissingUserId,
    #[error("Google identity exceeds PostgreSQL's 63-byte user-name limit")]
    UsernameTooLong,
    #[error("Google identity contains characters PostgreSQL cannot use in a user name")]
    InvalidUsername,
    #[error("PostgreSQL startup user does not match the Google identity")]
    UserMismatch,
}

#[cfg(test)]
mod tests {
    use std::{
        io::{Read, Write},
        net::TcpListener,
        thread,
    };

    use super::*;
    use crate::config::Settings;

    fn token_info() -> TokenInfo {
        TokenInfo {
            audience: Some("gcloud-client".into()),
            issued_to: None,
            user_id: Some("1234567890".into()),
            scope: Some("scope-a scope-b".into()),
            expires_in: Some(3600),
            email: Some("alice@example.com".into()),
            verified_email: Some(true),
        }
    }

    fn settings() -> Settings {
        Settings {
            require_tls: false,
            allowed_audiences: vec!["gcloud-client".into()],
            allowed_domains: vec!["example.com".into()],
            required_scopes: vec!["scope-a".into()],
            ..Default::default()
        }
    }

    fn mock_server(
        status: &'static str,
        body: &'static str,
    ) -> (String, thread::JoinHandle<String>) {
        let listener = TcpListener::bind("127.0.0.1:0").expect("bind mock server");
        let address = listener.local_addr().expect("mock server address");
        let handle = thread::spawn(move || {
            let (mut stream, _) = listener.accept().expect("accept request");
            let mut request = [0u8; 4096];
            let size = stream.read(&mut request).expect("read request");
            let request = String::from_utf8_lossy(&request[..size]).into_owned();
            let response = format!(
                "HTTP/1.1 {status}\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
                body.len()
            );
            stream
                .write_all(response.as_bytes())
                .expect("write response");
            request
        });

        (format!("http://{address}/tokeninfo"), handle)
    }

    #[test]
    fn accepts_verified_allowed_identity() {
        let username = validate(&settings(), "alice@example.com", token_info())
            .expect("token should validate");

        assert_eq!(username, "alice@example.com");
    }

    #[test]
    fn can_use_user_id_as_postgres_identity() {
        let settings = Settings {
            username_claim: UsernameClaim::UserId,
            require_user_match: false,
            require_verified_email: false,
            allowed_audiences: vec!["gcloud-client".into()],
            ..Default::default()
        };

        assert_eq!(
            validate(&settings, "ignored", token_info()).expect("token should validate"),
            "1234567890"
        );
    }

    #[test]
    fn rejects_expired_token() {
        let mut token = token_info();
        token.expires_in = Some(0);

        assert!(matches!(
            validate(&settings(), "alice@example.com", token),
            Err(AuthenticationError::Expired)
        ));
    }

    #[test]
    fn rejects_unverified_email() {
        let mut token = token_info();
        token.verified_email = Some(false);

        assert!(matches!(
            validate(&settings(), "alice@example.com", token),
            Err(AuthenticationError::UnverifiedEmail)
        ));
    }

    #[test]
    fn rejects_disallowed_audience_domain_and_scope() {
        let mut audience = settings();
        audience.allowed_audiences = vec!["different-client".into()];
        assert!(matches!(
            validate(&audience, "alice@example.com", token_info()),
            Err(AuthenticationError::AudienceNotAllowed)
        ));

        let mut domain = settings();
        domain.allowed_domains = vec!["other.example".into()];
        assert!(matches!(
            validate(&domain, "alice@example.com", token_info()),
            Err(AuthenticationError::PrincipalNotAllowed)
        ));

        let mut scope = settings();
        scope.required_scopes = vec!["scope-c".into()];
        assert!(matches!(
            validate(&scope, "alice@example.com", token_info()),
            Err(AuthenticationError::MissingScope)
        ));
    }

    #[test]
    fn rejects_startup_user_mismatch() {
        assert!(matches!(
            validate(&settings(), "bob@example.com", token_info()),
            Err(AuthenticationError::UserMismatch)
        ));
    }

    #[test]
    fn rejects_malformed_email_identity() {
        for email in ["alice", "@example.com", "alice@"] {
            let mut token = token_info();
            token.email = Some(email.into());

            assert!(matches!(
                validate(&settings(), email, token),
                Err(AuthenticationError::InvalidEmail)
            ));
        }
    }

    #[test]
    fn accepts_oidc_tokeninfo_field_names() {
        let token_info: TokenInfo = serde_json::from_str(
            r#"{
                "aud": "gcloud-client",
                "azp": "authorized-party",
                "sub": "1234567890",
                "scope": "scope-a",
                "expires_in": "3600",
                "email": "alice@example.com",
                "email_verified": "true"
            }"#,
        )
        .expect("parse tokeninfo response");

        assert_eq!(
            validate(&settings(), "alice@example.com", token_info).expect("token should validate"),
            "alice@example.com"
        );
    }

    #[test]
    fn accepts_boolean_and_missing_email_verified() {
        // Other Google identity endpoints send a real JSON boolean.
        let token_info: TokenInfo = serde_json::from_str(
            r#"{"expires_in": "3600", "email": "alice@example.com", "email_verified": true}"#,
        )
        .expect("parse boolean email_verified");
        assert_eq!(token_info.verified_email, Some(true));

        // Tokens without the email scope omit the field entirely.
        let token_info: TokenInfo =
            serde_json::from_str(r#"{"expires_in": "3600"}"#).expect("parse missing field");
        assert_eq!(token_info.verified_email, None);

        assert!(
            serde_json::from_str::<TokenInfo>(r#"{"expires_in": "3600", "email_verified": "yes"}"#)
                .is_err()
        );
    }

    #[test]
    fn accepts_numeric_and_string_expires_in() {
        // Google's tokeninfo stringifies it; other endpoints send a number.
        let token_info: TokenInfo =
            serde_json::from_str(r#"{"expires_in": 3599, "email": "alice@example.com"}"#)
                .expect("parse numeric expires_in");
        assert_eq!(token_info.expires_in, Some(3599));

        let token_info: TokenInfo =
            serde_json::from_str(r#"{"expires_in": "3599", "email": "alice@example.com"}"#)
                .expect("parse string expires_in");
        assert_eq!(token_info.expires_in, Some(3599));

        // A value that is neither is a response error, not a panic.
        assert!(
            serde_json::from_str::<TokenInfo>(r#"{"expires_in": "soon"}"#).is_err(),
            "a non-numeric expires_in must be rejected"
        );

        // Missing entirely: treated as no lifetime left.
        let token_info: TokenInfo =
            serde_json::from_str(r#"{"email": "alice@example.com"}"#).expect("parse without it");
        assert_eq!(token_info.expires_in, None);
        assert!(matches!(
            validate(&settings(), "alice@example.com", token_info),
            Err(AuthenticationError::Expired)
        ));
    }

    #[test]
    fn matches_the_startup_user_case_insensitively() {
        // `Settings::claims_user` lower-cases before deciding whether to
        // handle the login, so validation must not then reject the same user.
        assert!(settings().claims_user("Alice@Example.com"));
        assert_eq!(
            validate(&settings(), "Alice@Example.com", token_info()).expect("should validate"),
            "alice@example.com"
        );
    }

    #[test]
    fn rejects_identities_postgres_cannot_use() {
        for email in ["ali ce@example.com", "alice\u{7}@example.com"] {
            let mut token = token_info();
            token.email = Some(email.into());

            let mut settings = settings();
            settings.require_user_match = false;
            settings.allowed_domains = Vec::new();
            settings.allowed_emails = Vec::new();

            assert!(
                matches!(
                    validate(&settings, "ignored", token),
                    Err(AuthenticationError::InvalidUsername)
                ),
                "{email}"
            );
        }
    }

    #[test]
    fn calls_tokeninfo_without_leaking_token_in_errors() {
        let body = r#"{
          "azp": "42789329387.apps.googleusercontent.com",
          "aud": "42789329387.apps.googleusercontent.com",
          "sub": "427893293874278932938",
          "scope": "email https://www.googleapis.com/auth/accounts.reauth https://www.googleapis.com/auth/appengine.admin https://www.googleapis.com/auth/cloud-platform https://www.googleapis.com/auth/compute https://www.googleapis.com/auth/sqlservice.login https://www.googleapis.com/auth/userinfo.email openid",
          "exp": "1787664074",
          "expires_in": "2865",
          "email": "marco.palmisano@examplecompany.com",
          "email_verified": "true",
          "access_type": "offline"
        }"#;
        let (url, request) = mock_server("200 OK", body);
        let runtime = RuntimeConfig::from_settings(Settings {
            tokeninfo_url: url,
            require_tls: false,
            require_user_match: false,
            allowed_audiences: vec!["42789329387.apps.googleusercontent.com".into()],
            ..Default::default()
        })
        .expect("create runtime");
        let token = "ya29.a+b/c?";

        let grant = authenticate(&runtime, "ignored", token, false).expect("authenticate");
        assert_eq!(
            grant.derived_user.as_deref(),
            Some("marco.palmisano@examplecompany.com")
        );

        let request = request.join().expect("join mock server");
        assert!(request.starts_with("POST /tokeninfo"));
        // Nothing of the token in the request line, only in the form body.
        let (head, body) = request.split_once("\r\n\r\n").expect("request has a body");
        assert!(!head.contains("access_token"));
        assert!(body.starts_with("access_token="));
        assert!(!request.contains(token));
    }

    #[test]
    fn rejects_non_success_response() {
        let (url, request) = mock_server("400 Bad Request", r#"{"error":"invalid_token"}"#);
        let runtime = RuntimeConfig::from_settings(Settings {
            tokeninfo_url: url,
            require_tls: false,
            require_user_match: false,
            allowed_audiences: vec!["42789329387.apps.googleusercontent.com".into()],
            ..Default::default()
        })
        .expect("create runtime");

        assert!(matches!(
            authenticate(&runtime, "ignored", "bad-token", false),
            Err(AuthenticationError::Rejected)
        ));
        request.join().expect("join mock server");
    }
}
