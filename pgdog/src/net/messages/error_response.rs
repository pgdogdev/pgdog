//! ErrorResponse (B) message.
use std::fmt::Display;

use std::time::Duration;

use super::prelude::*;
use crate::{
    net::{c_string_buf, code},
    state::State,
};

/// ErrorResponse (B) message.
#[derive(Debug, Clone)]
pub struct ErrorResponse {
    pub severity: String,
    pub code: String,
    pub message: String,
    pub detail: Option<String>,
    pub context: Option<String>,
    pub file: Option<String>,
    pub routine: Option<String>,
}

impl Default for ErrorResponse {
    fn default() -> Self {
        Self {
            severity: "ERROR".into(),
            code: String::default(),
            message: String::default(),
            detail: None,
            context: None,
            file: None,
            routine: None,
        }
    }
}

impl ErrorResponse {
    /// Authentication error.
    pub fn auth(user: &str, database: &str) -> ErrorResponse {
        ErrorResponse {
            severity: "FATAL".into(),
            code: "28000".into(),
            message: format!(
                "password for user \"{}\" and database \"{}\" is wrong, or the database does not exist",
                user, database
            ),
            detail: None,
            context: None,
            file: None,
            routine: None,
        }
    }

    pub fn client_login_timeout(timeout: Duration) -> ErrorResponse {
        let mut error = Self::client_idle_timeout(timeout, &State::Active);
        error.message = "client login timeout".into();
        error.detail = Some(format!(
            "client_login_timeout of {}ms expired",
            timeout.as_millis()
        ));

        error
    }

    pub fn cross_shard_disabled(query: Option<&str>) -> ErrorResponse {
        ErrorResponse {
            severity: "ERROR".into(),
            code: "58000".into(),
            message: "cross-shard queries are disabled".into(),
            detail: Some(format!(
                "query doesn't have a sharding key{}",
                if let Some(query) = query {
                    format!(": {}", query)
                } else {
                    "".into()
                }
            )),
            context: None,
            file: None,
            routine: None,
        }
    }

    pub fn transaction_statement_mode() -> ErrorResponse {
        ErrorResponse {
            severity: "ERROR".into(),
            code: "58000".into(),
            message: "transaction control statements are not supported in statement pooler mode"
                .into(),
            ..Default::default()
        }
    }

    pub fn client_idle_timeout(duration: Duration, state: &State) -> ErrorResponse {
        ErrorResponse {
            severity: "FATAL".into(),
            code: "57P05".into(),
            message: format!(
                "disconnecting {} client",
                if state == &State::IdleInTransaction {
                    "idle in transaction"
                } else {
                    "idle"
                }
            ),
            detail: Some(format!(
                "{} of {}ms expired",
                if state == &State::IdleInTransaction {
                    "client_idle_in_transaction_timeout"
                } else {
                    "client_idle_timeout"
                },
                duration.as_millis()
            )),
            context: None,
            file: None,
            routine: None,
        }
    }

    /// Connection error.
    pub fn connection(user: &str, database: &str) -> ErrorResponse {
        ErrorResponse {
            severity: "ERROR".into(),
            code: "58000".into(),
            message: format!(
                r#"connection pool for user "{}" and database "{}" is down"#,
                user, database
            ),
            detail: None,
            context: None,
            file: None,
            routine: None,
        }
    }

    /// Pooler is shutting down.
    pub fn shutting_down() -> ErrorResponse {
        ErrorResponse {
            severity: "FATAL".into(),
            code: "57P01".into(),
            message: "PgDog is shutting down".into(),
            detail: None,
            context: None,
            file: None,
            routine: None,
        }
    }

    pub fn syntax(err: &str) -> ErrorResponse {
        Self {
            severity: "ERROR".into(),
            code: "42601".into(),
            message: err.into(),
            detail: None,
            context: None,
            file: None,
            routine: None,
        }
    }

    pub fn tls_required() -> ErrorResponse {
        Self {
            severity: "FATAL".into(),
            code: "08004".into(),
            message: "only TLS connections are allowed".into(),
            detail: None,
            context: None,
            file: None,
            routine: None,
        }
    }

    pub fn from_err(err: &impl std::error::Error) -> Self {
        let message = err.to_string();
        Self {
            severity: "ERROR".into(),
            code: "58000".into(),
            message,
            detail: None,
            context: None,
            file: None,
            routine: None,
        }
    }

    pub fn no_transaction() -> Self {
        Self {
            severity: "WARNING".into(),
            code: "25P01".into(),
            message: "there is no transaction in progress".into(),
            routine: Some("EndTransactionBlock".into()),
            file: Some("xact.c".into()),
            ..Default::default()
        }
    }

    pub fn in_failed_transaction() -> Self {
        Self {
            severity: "ERROR".into(),
            code: "25P02".into(),
            message:
                "current transaction is aborted, commands ignored until end of transaction block"
                    .into(),
            ..Default::default()
        }
    }
}

impl Display for ErrorResponse {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}: {} {}", self.severity, self.code, self.message)?;
        if let Some(ref detail) = self.detail {
            write!(f, "\n{}", detail)?
        }
        Ok(())
    }
}

impl FromBytes for ErrorResponse {
    fn from_bytes(mut bytes: Bytes) -> Result<Self, Error> {
        code!(bytes, 'E');

        let _len = bytes.get_i32();

        let mut error_response = ErrorResponse::default();

        while bytes.has_remaining() {
            let field = bytes.get_u8() as char;
            let value = c_string_buf(&mut bytes);

            match field {
                'S' => error_response.severity = value,
                'C' => error_response.code = value,
                'M' => error_response.message = value,
                'D' => error_response.detail = Some(value),
                'W' => error_response.context = Some(value),
                'F' => error_response.file = Some(value),
                'R' => error_response.routine = Some(value),
                _ => continue,
            }
        }

        Ok(error_response)
    }
}

impl ToBytes for ErrorResponse {
    fn to_bytes(&self) -> Result<Bytes, Error> {
        let mut payload = Payload::named(self.code());

        payload.put_u8(b'S');
        payload.put_string(&self.severity);

        payload.put_u8(b'V');
        payload.put_string(&self.severity);

        payload.put_u8(b'C');
        payload.put_string(&self.code);

        payload.put_u8(b'M');
        payload.put_string(&self.message);

        if let Some(ref detail) = self.detail {
            payload.put_u8(b'D');
            payload.put_string(detail);
        }

        if let Some(ref context) = self.context {
            payload.put_u8(b'W');
            payload.put_string(context);
        }

        if let Some(ref file) = self.file {
            payload.put_u8(b'F');
            payload.put_string(file);
        }

        if let Some(ref routine) = self.routine {
            payload.put_u8(b'R');
            payload.put_string(routine);
        }

        payload.put_u8(0);

        Ok(payload.freeze())
    }
}

impl Protocol for ErrorResponse {
    fn code(&self) -> char {
        'E'
    }
}
