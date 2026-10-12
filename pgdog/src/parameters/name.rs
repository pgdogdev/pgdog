use std::{
    borrow::Borrow,
    cmp::Ordering,
    fmt::{self, Display},
    hash::{Hash, Hasher},
    ops::Deref,
};

use crate::stats::memory::MemoryUsage;

/// An ASCII case-insensitive parameter name that preserves its original spelling.
#[derive(Debug, Clone)]
pub(crate) struct ParameterName {
    name: String,
}

impl ParameterName {
    pub(crate) fn new(name: impl Into<String>) -> Self {
        Self { name: name.into() }
    }
}

impl AsRef<str> for ParameterName {
    fn as_ref(&self) -> &str {
        &self.name
    }
}

impl Display for ParameterName {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.name)
    }
}

impl MemoryUsage for ParameterName {
    fn memory_usage(&self) -> usize {
        self.name.memory_usage()
    }
}

impl Deref for ParameterName {
    type Target = ParameterNameRef;

    fn deref(&self) -> &Self::Target {
        ParameterNameRef::new(&self.name)
    }
}

impl Borrow<ParameterNameRef> for ParameterName {
    fn borrow(&self) -> &ParameterNameRef {
        self
    }
}

impl Hash for ParameterName {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.deref().hash(state);
    }
}

impl PartialEq for ParameterName {
    fn eq(&self, other: &Self) -> bool {
        self.deref().eq(other.deref())
    }
}

impl Eq for ParameterName {}

impl PartialOrd for ParameterName {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for ParameterName {
    fn cmp(&self, other: &Self) -> Ordering {
        self.deref().cmp(other.deref())
    }
}

#[derive(Debug)]
#[repr(transparent)]
pub(crate) struct ParameterNameRef(str);

impl ParameterNameRef {
    pub(crate) fn new(name: &str) -> &Self {
        // SAFETY: This transparent wrapper has the same layout as str and no
        // additional validity requirements. The returned reference retains the
        // input's lifetime and slice length.
        unsafe { &*(name as *const str as *const Self) }
    }

    pub(crate) fn as_str(&self) -> &str {
        &self.0
    }
}

impl Hash for ParameterNameRef {
    fn hash<H: Hasher>(&self, state: &mut H) {
        for byte in self.0.bytes() {
            state.write_u8(byte.to_ascii_lowercase());
        }
        // Delimit names so hashing consecutive names is prefix-free. This byte
        // cannot occur in a UTF-8 string.
        state.write_u8(0xff);
    }
}

impl PartialEq for ParameterNameRef {
    fn eq(&self, other: &Self) -> bool {
        self.0.eq_ignore_ascii_case(&other.0)
    }
}

impl Eq for ParameterNameRef {}

impl PartialOrd for ParameterNameRef {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for ParameterNameRef {
    fn cmp(&self, other: &Self) -> Ordering {
        self.0
            .bytes()
            .map(|byte| byte.to_ascii_lowercase())
            .cmp(other.0.bytes().map(|byte| byte.to_ascii_lowercase()))
    }
}
