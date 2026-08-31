//! Authentication messages.

use crate::net::c_string_buf;

use super::{code, prelude::*};

use super::FromBytes;

pub(crate) mod password;
pub(crate) use password::Password;

/// Authentication messages.
#[derive(Debug)]
pub(crate) enum Authentication {
    /// AuthenticationOk (F)
    Ok,
    /// AuthenticationSASL (B)
    Sasl(Vec<String>),
    /// AuthenticationSASLContinue (B)
    SaslContinue(String),
    /// AuthenticationSASLFinal (B)
    SaslFinal(String),
    /// Md5 authentication challenge (B).
    Md5(Bytes),
    /// AuthenticationCleartextPassword (B).
    ClearTextPassword,
}

impl Authentication {
    pub(crate) const SCRAM_SHA_256: &'static str = "SCRAM-SHA-256";
    pub(crate) const SCRAM_SHA_256_PLUS: &'static str = "SCRAM-SHA-256-PLUS";

    /// Request SCRAM-SHA-256 auth (no channel binding).
    pub(crate) fn scram() -> Authentication {
        Authentication::Sasl(vec![Self::SCRAM_SHA_256.to_string()])
    }

    /// Request SCRAM-SHA-256-PLUS, falling back to SCRAM-SHA-256.
    ///
    /// PLUS is listed first so clients that pick the first advertised
    /// mechanism get channel binding, matching PostgreSQL.
    pub(crate) fn scram_plus() -> Authentication {
        Authentication::Sasl(vec![
            Self::SCRAM_SHA_256_PLUS.to_string(),
            Self::SCRAM_SHA_256.to_string(),
        ])
    }
}

pub(crate) fn scram_challenge(tls_server_end_point: Option<&[u8]>) -> Authentication {
    match tls_server_end_point {
        Some(_) => Authentication::scram_plus(),
        None => Authentication::scram(),
    }
}

impl FromBytes for Authentication {
    fn from_bytes(mut bytes: Bytes) -> Result<Self, Error> {
        code!(bytes, 'R');

        if bytes.remaining() < 8 {
            return Err(Error::UnexpectedPayload);
        }

        let _len = bytes.get_i32();

        let status = bytes.get_i32();

        match status {
            0 => Ok(Authentication::Ok),
            3 => Ok(Authentication::ClearTextPassword),
            5 => {
                if bytes.remaining() < 4 {
                    return Err(Error::UnexpectedPayload);
                }
                let mut salt = vec![0u8; 4];
                bytes.copy_to_slice(&mut salt);
                Ok(Authentication::Md5(Bytes::from(salt)))
            }
            10 => {
                let mut mechanisms = Vec::new();
                loop {
                    let mechanism = c_string_buf(&mut bytes);
                    if mechanism.is_empty() {
                        break;
                    }
                    mechanisms.push(mechanism);
                }
                if mechanisms.is_empty() {
                    return Err(Error::UnexpectedPayload);
                }
                Ok(Authentication::Sasl(mechanisms))
            }
            11 => {
                let data = c_string_buf(&mut bytes);
                Ok(Authentication::SaslContinue(data))
            }
            12 => {
                let data = c_string_buf(&mut bytes);
                Ok(Authentication::SaslFinal(data))
            }
            status => Err(Error::UnsupportedAuthentication(status)),
        }
    }
}

impl Protocol for Authentication {
    fn code(&self) -> char {
        'R'
    }
}

impl ToBytes for Authentication {
    fn to_bytes(&self) -> Bytes {
        let mut payload = Payload::named(self.code());

        match self {
            Authentication::Ok => {
                payload.put_i32(0);

                payload.freeze()
            }

            Authentication::ClearTextPassword => {
                payload.put_i32(3);
                payload.freeze()
            }

            Authentication::Md5(salt) => {
                payload.put_i32(5);
                payload.put(salt.clone());

                payload.freeze()
            }

            Authentication::Sasl(mechanisms) => {
                payload.put_i32(10);
                for mechanism in mechanisms {
                    payload.put_string(mechanism);
                }
                payload.put_u8(0);

                payload.freeze()
            }

            Authentication::SaslContinue(data) => {
                payload.put_i32(11);
                payload.put(Bytes::copy_from_slice(data.as_bytes()));

                payload.freeze()
            }

            Authentication::SaslFinal(data) => {
                payload.put_i32(12);
                payload.put(Bytes::copy_from_slice(data.as_bytes()));

                payload.freeze()
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn scram_advertises_sha_256_only() {
        let auth = Authentication::from_bytes(Authentication::scram().to_bytes()).unwrap();
        match auth {
            Authentication::Sasl(mechanisms) => {
                assert_eq!(mechanisms, vec![Authentication::SCRAM_SHA_256]);
            }
            other => panic!("expected Sasl, got {other:?}"),
        }
    }

    #[test]
    fn scram_plus_advertises_plus_then_sha_256() {
        let auth = Authentication::from_bytes(Authentication::scram_plus().to_bytes()).unwrap();
        match auth {
            Authentication::Sasl(mechanisms) => {
                assert_eq!(
                    mechanisms,
                    vec![
                        Authentication::SCRAM_SHA_256_PLUS,
                        Authentication::SCRAM_SHA_256
                    ]
                );
            }
            other => panic!("expected Sasl, got {other:?}"),
        }
    }

    #[test]
    fn scram_challenge_encodes_plus_then_sha_256_when_cbind_present() {
        let auth =
            Authentication::from_bytes(scram_challenge(Some(&[1, 2, 3])).to_bytes()).unwrap();
        match auth {
            Authentication::Sasl(mechanisms) => {
                assert_eq!(
                    mechanisms,
                    vec![
                        Authentication::SCRAM_SHA_256_PLUS,
                        Authentication::SCRAM_SHA_256
                    ]
                );
            }
            other => panic!("expected Sasl, got {other:?}"),
        }
    }

    #[test]
    fn scram_challenge_encodes_sha_256_only_when_cbind_absent() {
        let auth = Authentication::from_bytes(scram_challenge(None).to_bytes()).unwrap();
        match auth {
            Authentication::Sasl(mechanisms) => {
                assert_eq!(mechanisms, vec![Authentication::SCRAM_SHA_256]);
            }
            other => panic!("expected Sasl, got {other:?}"),
        }
    }

    #[test]
    fn sasl_without_mechanisms_is_unexpected_payload() {
        let mut payload = Payload::named('R');
        payload.put_i32(10);
        payload.put_u8(0);
        assert!(matches!(
            Authentication::from_bytes(payload.freeze()),
            Err(Error::UnexpectedPayload)
        ));
    }
}
