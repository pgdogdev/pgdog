//! Sharding key values returned by a plugin.

use std::{fmt, ops::Deref, ptr, slice};

/// A sharding key extracted from a query by a plugin.
///
/// The variant preserves the PostgreSQL data type so the receiver can pass the
/// value to the sharding function selected for the query.
#[derive(Debug, Clone, PartialEq, Eq)]
#[repr(C)]
pub enum Value {
    /// A `BIGINT` sharding key.
    Integer(i64),
    /// A `VARCHAR` sharding key.
    String(String),
    /// A `UUID` sharding key.
    Uuid(uuid::Uuid),
}

impl From<i64> for Value {
    fn from(value: i64) -> Self {
        Self::Integer(value)
    }
}

impl From<String> for Value {
    fn from(value: String) -> Self {
        Self::String(value)
    }
}

impl From<&str> for Value {
    fn from(value: &str) -> Self {
        Self::String(value.to_owned())
    }
}

impl From<uuid::Uuid> for Value {
    fn from(value: uuid::Uuid) -> Self {
        Self::Uuid(value)
    }
}

/// An owned array of sharding keys transferred from a plugin to PgDog.
///
/// This type abstracts the pointer and length used by the plugin ABI. It can
/// be used as a slice and releases its plugin-allocated values when dropped by
/// PgDog.
#[repr(C)]
pub struct ShardingKeys {
    data: *mut Value,
    len: usize,
}

impl ShardingKeys {
    /// Create an owned array of sharding keys.
    pub fn new(values: Vec<Value>) -> Self {
        let values = values.into_boxed_slice();
        let len = values.len();
        let data = Box::into_raw(values).cast::<Value>();

        Self { data, len }
    }

    /// Borrow the sharding keys as a slice.
    pub fn as_slice(&self) -> &[Value] {
        // SAFETY: `data` and `len` originate from the boxed slice created in
        // `Self::new`, or are the valid dangling/zero pair from `Self::default`.
        unsafe { slice::from_raw_parts(self.data, self.len) }
    }
}

impl Default for ShardingKeys {
    fn default() -> Self {
        Self {
            data: ptr::NonNull::dangling().as_ptr(),
            len: 0,
        }
    }
}

impl From<Vec<Value>> for ShardingKeys {
    fn from(values: Vec<Value>) -> Self {
        Self::new(values)
    }
}

impl Deref for ShardingKeys {
    type Target = [Value];

    fn deref(&self) -> &Self::Target {
        self.as_slice()
    }
}

impl AsRef<[Value]> for ShardingKeys {
    fn as_ref(&self) -> &[Value] {
        self.as_slice()
    }
}

impl fmt::Debug for ShardingKeys {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.as_slice().fmt(f)
    }
}

impl Drop for ShardingKeys {
    fn drop(&mut self) {
        // SAFETY: `data` is created exclusively by `Self::new` using
        // `Box::into_raw`. `ShardingKeys` cannot be cloned or copied, so this
        // reconstructs and drops the boxed slice exactly once.
        unsafe {
            drop(Box::from_raw(ptr::slice_from_raw_parts_mut(
                self.data, self.len,
            )));
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn owns_values_and_dereferences_to_a_slice() {
        let keys = ShardingKeys::new(vec![1_i64.into(), "tenant".into()]);

        assert_eq!(
            &*keys,
            &[Value::Integer(1), Value::String("tenant".to_owned())]
        );
    }

    #[test]
    fn default_is_empty() {
        assert!(ShardingKeys::default().is_empty());
    }
}
