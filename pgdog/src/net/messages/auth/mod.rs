//! Authentication messages.

use crate::net::c_string_buf;

use super::{code, prelude::*};

use super::FromBytes;

pub mod password;
pub use password::Password;

/// Authentication messages.
#[derive(Debug)]
pub enum Authentication {
    /// AuthenticationOk (F)
    Ok,
    /// AuthenticationSASL (B)
    Sasl(String),
    /// AuthenticationSASLContinue (B)
    SaslContinue(String),
    /// AuthenticationSASLFinal (B)
    SaslFinal(String),
    /// Md5 authentication challenge (B).
    Md5(Bytes),
    /// AuthenticationCleartextPassword (B).
    ClearTextPassword,
    /// AuthenticationGSS (B) - Code 7
    Gssapi,
    /// AuthenticationGSSContinue (B) - Code 8
    GssapiContinue(Vec<u8>),
    /// AuthenticationSSPI (B) - Code 9 (Windows)
    Sspi,
}

impl Authentication {
    /// Request SCRAM-SHA-256 auth.
    pub fn scram() -> Authentication {
        Authentication::Sasl("SCRAM-SHA-256".to_string())
    }
}

impl FromBytes for Authentication {
    fn from_bytes(mut bytes: Bytes) -> Result<Self, Error> {
        code!(bytes, 'R');

        let _len = bytes.get_i32();

        let status = bytes.get_i32();

        match status {
            0 => Ok(Authentication::Ok),
            3 => Ok(Authentication::ClearTextPassword),
            5 => {
                let mut salt = vec![0u8; 4];
                bytes.copy_to_slice(&mut salt);
                Ok(Authentication::Md5(Bytes::from(salt)))
            }
            7 => Ok(Authentication::Gssapi),
            8 => {
                let mut data = Vec::new();
                while bytes.has_remaining() {
                    data.push(bytes.get_u8());
                }
                Ok(Authentication::GssapiContinue(data))
            }
            9 => Ok(Authentication::Sspi),
            10 => {
                let mechanism = c_string_buf(&mut bytes);
                Ok(Authentication::Sasl(mechanism))
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
    fn to_bytes(&self) -> Result<Bytes, Error> {
        let mut payload = Payload::named(self.code());

        match self {
            Authentication::Ok => {
                payload.put_i32(0);

                Ok(payload.freeze())
            }

            Authentication::ClearTextPassword => {
                payload.put_i32(3);
                Ok(payload.freeze())
            }

            Authentication::Md5(salt) => {
                payload.put_i32(5);
                payload.put(salt.clone());

                Ok(payload.freeze())
            }

            Authentication::Sasl(mechanism) => {
                payload.put_i32(10);
                payload.put_string(mechanism);
                payload.put_u8(0);

                Ok(payload.freeze())
            }

            Authentication::SaslContinue(data) => {
                payload.put_i32(11);
                payload.put(Bytes::copy_from_slice(data.as_bytes()));

                Ok(payload.freeze())
            }

            Authentication::SaslFinal(data) => {
                payload.put_i32(12);
                payload.put(Bytes::copy_from_slice(data.as_bytes()));

                Ok(payload.freeze())
            }

            Authentication::Gssapi => {
                payload.put_i32(7);
                Ok(payload.freeze())
            }

            Authentication::GssapiContinue(data) => {
                payload.put_i32(8);
                payload.put(Bytes::from(data.clone()));
                Ok(payload.freeze())
            }

            Authentication::Sspi => {
                payload.put_i32(9);
                Ok(payload.freeze())
            }
        }
    }
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn test_gssapi_authentication() {
        let auth = Authentication::Gssapi;
        let bytes = auth.to_bytes().unwrap();
        let auth = Authentication::from_bytes(bytes).unwrap();
        match auth {
            Authentication::Gssapi => (),
            _ => panic!("Expected GSSAPI authentication"),
        }
    }

    #[test]
    fn test_gssapi_continue() {
        let data = vec![1, 2, 3, 4, 5];
        let auth = Authentication::GssapiContinue(data.clone());
        let bytes = auth.to_bytes().unwrap();
        let auth = Authentication::from_bytes(bytes).unwrap();
        match auth {
            Authentication::GssapiContinue(received_data) => {
                assert_eq!(received_data, data);
            }
            _ => panic!("Expected GssapiContinue authentication"),
        }
    }

    #[test]
    fn test_sspi_authentication() {
        let auth = Authentication::Sspi;
        let bytes = auth.to_bytes().unwrap();
        let auth = Authentication::from_bytes(bytes).unwrap();
        match auth {
            Authentication::Sspi => (),
            _ => panic!("Expected SSPI authentication"),
        }
    }

    #[test]
    fn test_gssapi_message_codes() {
        // Test that the correct protocol codes are used
        let gssapi = Authentication::Gssapi;
        let bytes = gssapi.to_bytes().unwrap();
        // Check for code 7 after the message header
        assert_eq!(bytes[5], 0); // First 3 bytes of i32(7) in big-endian
        assert_eq!(bytes[6], 0);
        assert_eq!(bytes[7], 0);
        assert_eq!(bytes[8], 7);

        let gssapi_continue = Authentication::GssapiContinue(vec![42]);
        let bytes = gssapi_continue.to_bytes().unwrap();
        // Check for code 8
        assert_eq!(bytes[5], 0); // First 3 bytes of i32(8) in big-endian
        assert_eq!(bytes[6], 0);
        assert_eq!(bytes[7], 0);
        assert_eq!(bytes[8], 8);

        let sspi = Authentication::Sspi;
        let bytes = sspi.to_bytes().unwrap();
        // Check for code 9
        assert_eq!(bytes[5], 0); // First 3 bytes of i32(9) in big-endian
        assert_eq!(bytes[6], 0);
        assert_eq!(bytes[7], 0);
        assert_eq!(bytes[8], 9);
    }
}
