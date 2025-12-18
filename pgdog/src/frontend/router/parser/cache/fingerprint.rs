use std::{fmt::Debug, ops::Deref};

use pg_query::fingerprint;

/// Query fingerprint.
pub struct Fingerprint {
    fingerprint: pg_query::Fingerprint,
}

impl Fingerprint {
    /// Fingerprint a query.
    pub(crate) fn new(query: &str) -> Result<Self, pg_query::Error> {
        Ok(Self {
            fingerprint: fingerprint(query)?,
        })
    }
}

impl Debug for Fingerprint {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Fingerprint")
            .field("value", &self.fingerprint.value)
            .field("hex", &self.fingerprint.hex)
            .finish()
    }
}

impl Default for Fingerprint {
    fn default() -> Self {
        Self {
            fingerprint: pg_query::Fingerprint {
                value: 0,
                hex: "".into(),
            },
        }
    }
}

impl Deref for Fingerprint {
    type Target = pg_query::Fingerprint;

    fn deref(&self) -> &Self::Target {
        &self.fingerprint
    }
}
