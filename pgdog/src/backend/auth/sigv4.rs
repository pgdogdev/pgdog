//! RDS IAM auth token signing (AWS SigV4, presigned query string) using
//! AWS-LC for SHA-256 and HMAC-SHA256.
//!
//! `aws-sdk-rds`'s `AuthTokenGenerator` signs through `aws-sigv4`, which hashes
//! with RustCrypto rather than a validated module. FIPS builds sign here
//! instead; the output is byte-for-byte what the SDK produces.
//!
//! <https://docs.aws.amazon.com/IAM/latest/UserGuide/create-signed-request.html>

use std::time::{Duration, SystemTime};

use aws_lc_rs::digest::{SHA256, digest};
use aws_lc_rs::hmac::{HMAC_SHA256, Key, sign};
use chrono::{DateTime, Utc};
use url::Url;

use crate::backend::Error;

const ALGORITHM: &str = "AWS4-HMAC-SHA256";
const SERVICE: &str = "rds-db";
const ACTION: &str = "connect";
/// RDS IAM tokens are valid for at most 15 minutes.
pub(crate) const EXPIRES_IN: Duration = Duration::from_secs(900);

/// The database login a token is minted for.
pub(crate) struct TokenRequest<'a> {
    pub(crate) host: &'a str,
    pub(crate) port: u16,
    pub(crate) user: &'a str,
    pub(crate) region: &'a str,
}

/// AWS credentials to sign with.
pub(crate) struct SigningCredentials<'a> {
    pub(crate) access_key_id: &'a str,
    pub(crate) secret_access_key: &'a str,
    pub(crate) session_token: Option<&'a str>,
}

/// Generate an RDS IAM auth token: a SigV4-presigned
/// `GET https://host:port/?Action=connect&DBUser=user` without the scheme.
pub(crate) fn rds_auth_token(
    request: &TokenRequest<'_>,
    credentials: &SigningCredentials<'_>,
    time: SystemTime,
) -> Result<String, Error> {
    let time = DateTime::<Utc>::from(time);
    let amz_date = time.format("%Y%m%dT%H%M%SZ").to_string();
    let date = time.format("%Y%m%d").to_string();
    let scope = format!("{date}/{}/{SERVICE}/aws4_request", request.region);
    let credential = format!("{}/{scope}", credentials.access_key_id);
    let expires = EXPIRES_IN.as_secs().to_string();

    let mut url = Url::parse(&format!(
        "https://{}:{}/?Action={ACTION}&DBUser={}",
        request.host, request.port, request.user
    ))
    .map_err(|err| Error::RdsIamToken(format!("invalid RDS IAM token URL: {err}")))?;

    let mut params: Vec<(String, String)> = url.query_pairs().into_owned().collect();
    params.extend(
        [
            ("X-Amz-Date", amz_date.as_str()),
            ("X-Amz-Expires", expires.as_str()),
            ("X-Amz-Algorithm", ALGORITHM),
            ("X-Amz-Credential", credential.as_str()),
            ("X-Amz-SignedHeaders", "host"),
        ]
        .into_iter()
        .chain(
            credentials
                .session_token
                .map(|token| ("X-Amz-Security-Token", token)),
        )
        .map(|(key, value)| (key.to_owned(), value.to_owned())),
    );

    let canonical_request = format!(
        "GET\n/\n{}\nhost:{}\n\nhost\n{}",
        canonical_query(&params),
        host_header(request.host, request.port),
        hex::encode(digest(&SHA256, b"")),
    );
    let string_to_sign = format!(
        "{ALGORITHM}\n{amz_date}\n{scope}\n{}",
        hex::encode(digest(&SHA256, canonical_request.as_bytes())),
    );
    let signature = hex::encode(hmac(
        &signing_key(
            credentials.secret_access_key,
            &date,
            request.region,
            SERVICE,
        ),
        string_to_sign.as_bytes(),
    ));

    {
        let mut query = url.query_pairs_mut();
        query
            .append_pair("X-Amz-Algorithm", ALGORITHM)
            .append_pair("X-Amz-Credential", &credential)
            .append_pair("X-Amz-Date", &amz_date)
            .append_pair("X-Amz-Expires", &expires)
            .append_pair("X-Amz-SignedHeaders", "host")
            .append_pair("X-Amz-Signature", &signature);
        if let Some(token) = credentials.session_token {
            query.append_pair("X-Amz-Security-Token", token);
        }
    }

    let url = url.to_string();
    Ok(url
        .strip_prefix("https://")
        .map(str::to_owned)
        .unwrap_or(url))
}

/// Query parameters URI-encoded and sorted by encoded key, then value.
fn canonical_query(params: &[(String, String)]) -> String {
    let mut encoded = params
        .iter()
        .map(|(key, value)| (uri_encode(key), uri_encode(value)))
        .collect::<Vec<_>>();
    encoded.sort();

    encoded
        .iter()
        .map(|(key, value)| format!("{key}={value}"))
        .collect::<Vec<_>>()
        .join("&")
}

/// SigV4 URI encoding: everything except `A-Za-z0-9-_.~` is percent-encoded.
fn uri_encode(value: &str) -> String {
    let mut encoded = String::with_capacity(value.len());
    for byte in value.bytes() {
        if byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_' | b'.' | b'~') {
            encoded.push(byte as char);
        } else {
            encoded.push_str(&format!("%{byte:02X}"));
        }
    }
    encoded
}

/// `Host` header as signed: the default HTTPS port is omitted.
fn host_header(host: &str, port: u16) -> String {
    if port == 443 {
        host.to_owned()
    } else {
        format!("{host}:{port}")
    }
}

fn signing_key(secret_access_key: &str, date: &str, region: &str, service: &str) -> Vec<u8> {
    let key = hmac(
        format!("AWS4{secret_access_key}").as_bytes(),
        date.as_bytes(),
    );
    let key = hmac(&key, region.as_bytes());
    let key = hmac(&key, service.as_bytes());
    hmac(&key, b"aws4_request")
}

fn hmac(key: &[u8], data: &[u8]) -> Vec<u8> {
    sign(&Key::new(HMAC_SHA256, key), data).as_ref().to_vec()
}

#[cfg(test)]
mod tests {
    use std::time::UNIX_EPOCH;

    use aws_config::{Region, SdkConfig};
    use aws_credential_types::{Credentials, provider::SharedCredentialsProvider};
    use aws_sdk_rds::auth_token::{AuthTokenGenerator, Config as AuthTokenConfig};
    use aws_smithy_async::time::StaticTimeSource;

    use super::*;

    const AUG_26_2024: u64 = 1_724_709_600;

    fn credentials<'a>(session_token: Option<&'a str>) -> SigningCredentials<'a> {
        SigningCredentials {
            access_key_id: "akid",
            secret_access_key: "secret",
            session_token,
        }
    }

    /// The token `aws-sdk-rds` generates for the same inputs.
    async fn sdk_token(
        request: &TokenRequest<'_>,
        session_token: Option<&str>,
        time: u64,
    ) -> String {
        let sdk_config = SdkConfig::builder()
            .credentials_provider(SharedCredentialsProvider::new(Credentials::new(
                "akid",
                "secret",
                session_token.map(str::to_owned),
                None,
                "test",
            )))
            .time_source(StaticTimeSource::from_secs(time))
            .build();
        let config = AuthTokenConfig::builder()
            .hostname(request.host)
            .port(request.port.into())
            .username(request.user)
            .region(Region::new(request.region.to_owned()))
            .build()
            .unwrap();

        AuthTokenGenerator::new(config)
            .auth_token(&sdk_config)
            .await
            .unwrap()
            .to_string()
    }

    #[test]
    fn test_aws_sdk_test_vector() {
        // aws-sdk-rds's own `signing_works` test vector.
        let request = TokenRequest {
            host: "prod-instance.us-east-1.rds.amazonaws.com",
            port: 3306,
            user: "peccy",
            region: "us-east-1",
        };
        let time = UNIX_EPOCH + Duration::from_secs(AUG_26_2024);

        assert_eq!(
            rds_auth_token(&request, &credentials(None), time).unwrap(),
            "prod-instance.us-east-1.rds.amazonaws.com:3306/?Action=connect&DBUser=peccy&X-Amz-Algorithm=AWS4-HMAC-SHA256&X-Amz-Credential=akid%2F20240826%2Fus-east-1%2Frds-db%2Faws4_request&X-Amz-Date=20240826T220000Z&X-Amz-Expires=900&X-Amz-SignedHeaders=host&X-Amz-Signature=dd0cba843009474347af724090233265628ace491ea17ce3eb3da098b983ad89"
        );
    }

    #[tokio::test]
    async fn test_matches_sdk() {
        let requests = [
            TokenRequest {
                host: "db.cluster-abc123.us-east-1.rds.amazonaws.com",
                port: 5432,
                user: "db_user",
                region: "us-east-1",
            },
            // Mixed-case host, default HTTPS port.
            TokenRequest {
                host: "DB.cluster-abc123.eu-west-1.rds.amazonaws.com",
                port: 443,
                user: "app",
                region: "eu-west-1",
            },
            // Characters that need encoding in the user name. (The SDK panics
            // on a space, so that case is tested on its own below.)
            TokenRequest {
                host: "db.cn-north-1.rds.amazonaws.com.cn",
                port: 5432,
                user: "app+ops@team~1.x",
                region: "cn-north-1",
            },
        ];
        // STS session tokens contain `/`, `+` and `=`.
        let session_tokens = [None, Some("FwoGZXIvYXdzE+example/token=="), Some("simple")];

        for request in &requests {
            for session_token in session_tokens {
                for time in [AUG_26_2024, 1_783_036_799] {
                    let ours = rds_auth_token(
                        request,
                        &credentials(session_token),
                        UNIX_EPOCH + Duration::from_secs(time),
                    )
                    .unwrap();
                    let sdk = sdk_token(request, session_token, time).await;
                    assert_eq!(
                        ours, sdk,
                        "user {:?}, token {session_token:?}",
                        request.user
                    );
                }
            }
        }
    }

    #[test]
    fn test_user_with_space() {
        // `aws-sdk-rds` panics on this user name; signing it must still work.
        let request = TokenRequest {
            host: "db.us-east-1.rds.amazonaws.com",
            port: 5432,
            user: "app user",
            region: "us-east-1",
        };
        let time = UNIX_EPOCH + Duration::from_secs(AUG_26_2024);
        let token = rds_auth_token(&request, &credentials(None), time).unwrap();

        assert!(
            token.starts_with(
                "db.us-east-1.rds.amazonaws.com:5432/?Action=connect&DBUser=app%20user&"
            ),
            "{token}"
        );
        assert_eq!(
            token,
            rds_auth_token(&request, &credentials(None), time).unwrap()
        );
    }

    #[test]
    fn test_uri_encode() {
        assert_eq!(uri_encode("AZaz09-_.~"), "AZaz09-_.~");
        assert_eq!(uri_encode("a b/c+d=e:f"), "a%20b%2Fc%2Bd%3De%3Af");
        assert_eq!(uri_encode("é"), "%C3%A9");
    }

    #[test]
    fn test_signing_key_aws_example() {
        // AWS SigV4 documentation: "Examples of how to derive a signing key".
        let key = signing_key(
            "wJalrXUtnFEMI/K7MDENG+bPxRfiCYEXAMPLEKEY",
            "20120215",
            "us-east-1",
            "iam",
        );
        assert_eq!(
            hex::encode(key),
            "f4780e2d9f65fa895f9c67b32ce1baf0b0d8a43505a000a1a9e090d414db404d"
        );
    }

    #[test]
    fn test_invalid_host_is_an_error() {
        let request = TokenRequest {
            host: "bad host",
            port: 5432,
            user: "u",
            region: "us-east-1",
        };
        assert!(matches!(
            rds_auth_token(&request, &credentials(None), SystemTime::now()),
            Err(Error::RdsIamToken(_))
        ));
    }
}
