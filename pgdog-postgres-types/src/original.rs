use bytes::Bytes;

use crate::Format;

/// Wire bytes exactly as Postgres sent them.
///
/// Some types print differently depending on session settings
/// (`lc_monetary`, `DateStyle`), so re-encoding a parsed value can't
/// reproduce the server's output. Keeping the original bytes lets merged
/// results go back to the client unchanged.
#[derive(Debug, Clone)]
pub(crate) struct Original {
    bytes: Bytes,
    format: Format,
}

impl Original {
    pub(crate) fn new(bytes: &[u8], format: Format) -> Self {
        Self {
            bytes: Bytes::copy_from_slice(bytes),
            format,
        }
    }

    /// The original bytes, if they are in the requested format.
    pub(crate) fn get(&self, format: Format) -> Option<Bytes> {
        (self.format == format).then(|| self.bytes.clone())
    }

    pub(crate) fn bytes(&self) -> &Bytes {
        &self.bytes
    }
}
