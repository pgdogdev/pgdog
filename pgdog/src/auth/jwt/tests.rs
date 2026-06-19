use super::pem_cache::get_static_pem_bytes;
use super::*;
use jsonwebtoken::{Algorithm, EncodingKey, Header};
use std::time::{SystemTime, UNIX_EPOCH};

const TEST_PRIVATE_KEY_PEM: &[u8] = b"-----BEGIN PRIVATE KEY-----
MIIEvgIBADANBgkqhkiG9w0BAQEFAASCBKgwggSkAgEAAoIBAQCmYC6Np7JXYe7a
PBv7SB84oz3Cx8mqVFFgNSvqTsqROT3NCK8aEtnPwsStxYpkXw7cKD2M6qFx7Y12
CpoNQWL12dY0x3MPDWV+uQTejbklq5NDOh9IID7+RqNMO/t8/yKwY/HzJutOU2SV
CTFlJKG8KUJOQTFnWYC+rOcMk3L1befIdyQM3IDD2zBAJtDGTClZBrrjkEZmsb39
lBsTM3pkzUIRcxpeFwITmTZeR/CqQpU3J/3aSqZt6EhTXheIotl3wdQYT28XpbcY
TioVfNDhQhwBWwiGi6L70Q17EprClXdCzueHHQkzkuTQXHxWOj1k8EHU73QVyXUq
VSx1TjwvAgMBAAECggEAO6rP10aWi4cYQZUAFgC6DbZhjmrXNKpTmtTG4JuMQ0PL
ma4tGgU7rypzHbz0EmYS7rrRxClbZ//hVT2dHPbftjr++uOyrGnKBgX1rJkYFt3v
DNOZ52SFIu0TYGI8oYngl3DokyLYjbkTn+1xlQvrow8K9ASmYqGzLe7VV+nDdyf0
w3xPHigjXsSr+j0INa5IXe7HGDsCFAIOfAg0ruGPKEkKg1Sxwq6/BFr8Rq28kaMv
wlMtHDyqfxXezf3JPjoe9hnNCi6d6n/buI/5qYHMR+r4Eb5iZbMzCEeOz5BT26Ku
YHa36jJC3b5z7N1B1g2DgPYWIyvsu1LnEGfFJaHmSQKBgQDSS85TXvxMM0A419Jf
LzVjWSN4dkqoJvM5/4ra7BB4/2353FP/TeSkGLARZ0WfpvFLvk/iTeH7IyMQTEB3
xnoWT/kuPhLscwDwRZRgr0O+S6BFNMzGafI/SaD/TUy1lize9EKK2zYRsfT+EmTB
0+hX5ILMxGqy3AqcH7TtcHTnKQKBgQDKiMlw5TcZ84gLW3HMVwiBIIu5xTUUrAq1
65al69lWR2woma9eg2a70ES+oJpLkuM4FTXZq0/X/XXgmAQxszu091a6GdPb5XV8
lYx31xpBiZwyeB2y5yEbSq+AawzFMIUWmRH94+mRv2+wsUmUZ84TVHua22A3+Qur
l1/GoBYrlwKBgQDDie09pFKgX/9VW4inLPRNfnL27bcZh64dvblVOq9OcuPFstL/
z2PMGZCNfiNFAivXrAwHdzerFs7htqUzOgAHgzFFiD58UasLvwbqp80rwpIyB5ho
3dZ8dnAXM78iEZODdEfzaUVrSrdtD5lUiT+/iiD9WZ2E1gmfhfPr2+c3kQKBgHvf
9fVK/MyumwL3Rz8H7HeuBEf3SmP+Zf6mvVl2S1PuE0Ux2oUgMXGmDKXbbQPUL41Z
y7n6gbdFmxdnYwlS6q3gqfbhXScdzSIKBgQ2WCTFmfd0aBXIMAOVRopw7zqcVopf
zRVQlMdEI3gatzpB01UXUxKAIvWZKX4l87p0p5q5AoGBALh91ATgdrEhe16TV0ru
8B89ZtMcgqaKXVlPO/Rr6fBT/VhSW9KV52sik4Jea5AFfZty/DzhP8pXbJtZjHjA
d5Bja4qZrKG67E92JhoCKMso/JaQELsln9GzQVkVsgLxF1IP0k797znJIfHDFvma
ntQFcU46jCHtpmL7OyzV4NoT
-----END PRIVATE KEY-----";

const TEST_PUBLIC_KEY_PEM: &[u8] = b"-----BEGIN PUBLIC KEY-----
MIIBIjANBgkqhkiG9w0BAQEFAAOCAQ8AMIIBCgKCAQEApmAujaeyV2Hu2jwb+0gf
OKM9wsfJqlRRYDUr6k7KkTk9zQivGhLZz8LErcWKZF8O3Cg9jOqhce2NdgqaDUFi
9dnWNMdzDw1lfrkE3o25JauTQzofSCA+/kajTDv7fP8isGPx8ybrTlNklQkxZSSh
vClCTkExZ1mAvqznDJNy9W3nyHckDNyAw9swQCbQxkwpWQa645BGZrG9/ZQbEzN6
ZM1CEXMaXhcCE5k2XkfwqkKVNyf92kqmbehIU14XiKLZd8HUGE9vF6W3GE4qFXzQ
4UIcAVsIhoui+9ENexKawpV3Qs7nhx0JM5Lk0Fx8Vjo9ZPBB1O90Fcl1KlUsdU48
LwIDAQAB
-----END PUBLIC KEY-----";

fn current_time_secs() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_secs()
}

fn generate_token(claims: &Claims, alg: Algorithm, key: &[u8]) -> String {
    let header = Header::new(alg);
    let key = EncodingKey::from_rsa_pem(key).unwrap();
    jsonwebtoken::encode(&header, claims, &key).unwrap()
}

#[tokio::test]
async fn test_jwt_validation_success() {
    let temp_dir = tempfile::tempdir().unwrap();
    let pub_key_path = temp_dir.path().join("test.pub.pem");
    std::fs::write(&pub_key_path, TEST_PUBLIC_KEY_PEM).unwrap();

    let general = General {
        jwt_public_key_file: Some(pub_key_path.to_str().unwrap().to_string()),
        jwt_audience: Some("test_audience".to_string()),
        ..Default::default()
    };

    let validator = JwtValidator::new(&general).unwrap();

    let exp = current_time_secs() + 60;
    let claims = Claims {
        sub: "postgres_user".to_string(),
        exp,
        aud: Some(serde_json::Value::String("test_audience".to_string())),
        extra: std::collections::HashMap::new(),
    };

    let token = generate_token(&claims, Algorithm::RS256, TEST_PRIVATE_KEY_PEM);
    let decoded = validator.validate(&token).await.unwrap();

    assert_eq!(decoded.sub, "postgres_user");
    assert_eq!(decoded.exp, exp);
}

#[tokio::test]
async fn test_jwt_validation_expired() {
    let temp_dir = tempfile::tempdir().unwrap();
    let pub_key_path = temp_dir.path().join("test.pub.pem");
    std::fs::write(&pub_key_path, TEST_PUBLIC_KEY_PEM).unwrap();

    let general = General {
        jwt_public_key_file: Some(pub_key_path.to_str().unwrap().to_string()),
        ..Default::default()
    };

    let validator = JwtValidator::new(&general).unwrap();

    // Only 10 seconds in the past: this is within `jsonwebtoken`'s default
    // 60s leeway, so it would be accepted unless leeway is set to 0.
    let exp = current_time_secs() - 10;
    let claims = Claims {
        sub: "postgres_user".to_string(),
        exp,
        aud: None,
        extra: std::collections::HashMap::new(),
    };

    let token = generate_token(&claims, Algorithm::RS256, TEST_PRIVATE_KEY_PEM);
    let err = validator.validate(&token).await.unwrap_err();

    assert!(matches!(err, JwtError::TokenExpired));
}

#[tokio::test]
async fn test_jwt_validation_wrong_algorithm() {
    let temp_dir = tempfile::tempdir().unwrap();
    let pub_key_path = temp_dir.path().join("test.pub.pem");
    std::fs::write(&pub_key_path, TEST_PUBLIC_KEY_PEM).unwrap();

    let general = General {
        jwt_public_key_file: Some(pub_key_path.to_str().unwrap().to_string()),
        ..Default::default()
    };

    let validator = JwtValidator::new(&general).unwrap();

    let exp = current_time_secs() + 60;
    let claims = Claims {
        sub: "postgres_user".to_string(),
        exp,
        aud: None,
        extra: std::collections::HashMap::new(),
    };

    let token = generate_token(&claims, Algorithm::RS512, TEST_PRIVATE_KEY_PEM);
    let err = validator.validate(&token).await.unwrap_err();

    assert!(matches!(err, JwtError::InvalidAlgorithm));
}

#[tokio::test]
async fn test_jwt_validation_missing_sub() {
    let temp_dir = tempfile::tempdir().unwrap();
    let pub_key_path = temp_dir.path().join("test.pub.pem");
    std::fs::write(&pub_key_path, TEST_PUBLIC_KEY_PEM).unwrap();

    let general = General {
        jwt_public_key_file: Some(pub_key_path.to_str().unwrap().to_string()),
        ..Default::default()
    };

    let validator = JwtValidator::new(&general).unwrap();

    let exp = current_time_secs() + 60;
    let claims = Claims {
        sub: "".to_string(),
        exp,
        aud: None,
        extra: std::collections::HashMap::new(),
    };

    let token = generate_token(&claims, Algorithm::RS256, TEST_PRIVATE_KEY_PEM);
    let err = validator.validate(&token).await.unwrap_err();

    assert!(matches!(err, JwtError::MissingSub));
}

#[tokio::test]
async fn test_jwt_validation_audience_mismatch() {
    let temp_dir = tempfile::tempdir().unwrap();
    let pub_key_path = temp_dir.path().join("test.pub.pem");
    std::fs::write(&pub_key_path, TEST_PUBLIC_KEY_PEM).unwrap();

    let general = General {
        jwt_public_key_file: Some(pub_key_path.to_str().unwrap().to_string()),
        jwt_audience: Some("expected_audience".to_string()),
        ..Default::default()
    };

    let validator = JwtValidator::new(&general).unwrap();

    let exp = current_time_secs() + 60;
    let claims = Claims {
        sub: "postgres_user".to_string(),
        exp,
        aud: Some(serde_json::Value::String("wrong_audience".to_string())),
        extra: std::collections::HashMap::new(),
    };

    let token = generate_token(&claims, Algorithm::RS256, TEST_PRIVATE_KEY_PEM);
    let err = validator.validate(&token).await.unwrap_err();

    assert!(matches!(err, JwtError::AudienceMismatch));
}

#[tokio::test]
async fn test_jwt_validator_memory_leak_prevention() {
    let temp_dir = tempfile::tempdir().unwrap();
    let pub_key_path = temp_dir.path().join("test_mem.pub.pem");
    std::fs::write(&pub_key_path, TEST_PUBLIC_KEY_PEM).unwrap();

    let path_str = pub_key_path.to_str().unwrap();

    let first_load = get_static_pem_bytes(path_str).unwrap();
    let second_load = get_static_pem_bytes(path_str).unwrap();

    assert_eq!(
        first_load.as_ptr(),
        second_load.as_ptr(),
        "Cache did not reuse leaked memory pointer!"
    );

    let alternate_pem = b"-----BEGIN PUBLIC KEY-----\nALTERNATE_CONTENT\n-----END PUBLIC KEY-----";
    std::fs::write(&pub_key_path, alternate_pem).unwrap();

    let third_load = get_static_pem_bytes(path_str).unwrap();
    assert_ne!(
        first_load.as_ptr(),
        third_load.as_ptr(),
        "Cache returned stale pointer after disk file modification!"
    );
}

#[test]
fn test_jwt_user_suffix_matching() {
    // 1. With configured suffix
    let suffix = Some("@example.com".to_string());

    let user_sso = "john.doe@example.com";
    let user_other = "datastream";

    let match_sso = match suffix {
        Some(ref s) => user_sso.ends_with(s.as_str()),
        None => true,
    };
    let match_other = match suffix {
        Some(ref s) => user_other.ends_with(s.as_str()),
        None => true,
    };

    assert!(match_sso, "SSO suffix should have matched!");
    assert!(!match_other, "Non-SSO suffix should not have matched!");

    // 2. Without configured suffix (should match all)
    let no_suffix: Option<String> = None;
    let match_no_suffix_sso = match no_suffix {
        Some(ref s) => user_sso.ends_with(s.as_str()),
        None => true,
    };
    let match_no_suffix_other = match no_suffix {
        Some(ref s) => user_other.ends_with(s.as_str()),
        None => true,
    };

    assert!(
        match_no_suffix_sso,
        "Should default to true when suffix is None!"
    );
    assert!(
        match_no_suffix_other,
        "Should default to true when suffix is None!"
    );
}

#[tokio::test]
async fn test_jwt_custom_username_claim() {
    let temp_dir = tempfile::tempdir().unwrap();
    let pub_key_path = temp_dir.path().join("test_claim.pub.pem");
    std::fs::write(&pub_key_path, TEST_PUBLIC_KEY_PEM).unwrap();

    let general = General {
        jwt_public_key_file: Some(pub_key_path.to_str().unwrap().to_string()),
        jwt_username_claim: "email".to_string(),
        ..Default::default()
    };

    let validator = JwtValidator::new(&general).unwrap();

    let exp = current_time_secs() + 60;
    let mut claims = Claims {
        sub: "postgres_user_sub".to_string(),
        exp,
        aud: None,
        extra: std::collections::HashMap::new(),
    };
    claims.extra.insert(
        "email".to_string(),
        serde_json::Value::String("postgres_user_email@example.com".to_string()),
    );

    let token = generate_token(&claims, Algorithm::RS256, TEST_PRIVATE_KEY_PEM);
    let decoded = validator.validate(&token).await.unwrap();

    let username = decoded.get_username(&general.jwt_username_claim).unwrap();
    assert_eq!(username, "postgres_user_email@example.com");
}
