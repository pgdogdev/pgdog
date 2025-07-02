//! Use LZ4 to compress C-Strings
//! we got from Postgres so we can store them in memory more efficiently.
use std::{fmt::Debug, str::from_utf8_unchecked};

use bytes::Bytes;
use lz4_flex::{compress_prepend_size, decompress_size_prepended};

use crate::net::Error;

#[derive(Clone, Hash, Eq, PartialEq, Default)]
pub struct CompressedString {
    payload: Bytes,
    compressed: bool,
}

impl Debug for CompressedString {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let mut me = self.clone();
        let s = if let Ok(decompressed) = me.decompress() {
            decompressed.value().to_string()
        } else {
            "<decompression failed>".to_string()
        };

        f.debug_struct("CompresedString")
            .field("value", &s)
            .finish()
    }
}

impl From<Bytes> for CompressedString {
    fn from(value: Bytes) -> Self {
        Self::new(value)
    }
}

impl CompressedString {
    pub fn new(payload: Bytes) -> Self {
        Self {
            payload,
            compressed: false,
        }
    }

    pub fn len(&self) -> usize {
        if self.compressed {
            u32::from_le_bytes(self.payload[0..4].try_into().unwrap()) as usize
        } else {
            self.payload().len()
        }
    }

    pub fn compress(&mut self) {
        if !self.compressed {
            let compressed = compress_prepend_size(&self.payload);
            self.payload = Bytes::from(compressed);
            self.compressed = true;
        }
    }

    pub fn decompress(&self) -> Result<DecompressedString, Error> {
        if self.compressed {
            let payload = decompress_size_prepended(&self.payload)?;
            Ok(DecompressedString {
                payload: Bytes::from(payload),
                recompress: true,
            })
        } else {
            Ok(DecompressedString {
                payload: self.payload.clone(),
                recompress: false,
            })
        }
    }

    pub fn payload(&self) -> &Bytes {
        &self.payload
    }
}

pub struct DecompressedString {
    payload: Bytes,
    recompress: bool,
}

impl DecompressedString {
    pub fn value(&self) -> &str {
        // SAFETY: We only allow UTF-8 strings.
        unsafe { from_utf8_unchecked(&self.payload[0..self.payload.len() - 1]) }
    }

    pub fn payload(&self) -> &Bytes {
        &self.payload
    }
}

#[cfg(test)]
mod test {
    use bytes::Bytes;

    use crate::net::compressed_string::CompressedString;

    #[test]
    fn test_compress_decompress() {
        let s = "SELECT * FROM users WHERE id = $1 AND values IN ($1, $2, $3, $4, $5, $6)\0";
        let mut maybe_compressed = CompressedString::new(Bytes::from(s));

        for _ in 0..25 {
            assert_eq!(
                maybe_compressed.decompress().unwrap().value(),
                &s[0..s.len() - 1]
            );
            maybe_compressed.compress();

            assert_eq!(
                maybe_compressed.decompress().unwrap().value(),
                &s[0..s.len() - 1]
            );
        }
    }
}
