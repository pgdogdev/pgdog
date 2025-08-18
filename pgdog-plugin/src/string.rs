//! Wrapper around Rust's [`str`], a UTF-8 encoded slice.
//!
//! This is used to pass strings back and forth between the plugin and
//! PgDog, without allocating memory as required by [`std::ffi::CString`].
//!
//! ### Example
//!
//! ```
//! use pgdog_plugin::PdStr;
//! use std::ops::Deref;
//!
//! let string = PdStr::from("hello world");
//! assert_eq!(string.deref(), "hello world");
//!
//! let string = string.to_string(); // Owned version.
//! ```
//!
use crate::bindings::PdStr;
use std::{ops::Deref, os::raw::c_void, slice::from_raw_parts, str::from_utf8_unchecked};

impl From<&str> for PdStr {
    fn from(value: &str) -> Self {
        PdStr {
            data: value.as_ptr() as *mut c_void,
            len: value.len(),
        }
    }
}

impl From<&String> for PdStr {
    fn from(value: &String) -> Self {
        PdStr {
            data: value.as_ptr() as *mut c_void,
            len: value.len(),
        }
    }
}

impl Deref for PdStr {
    type Target = str;

    fn deref(&self) -> &Self::Target {
        unsafe {
            let slice = from_raw_parts::<u8>(self.data as *mut u8, self.len);
            from_utf8_unchecked(slice)
        }
    }
}

impl PartialEq for PdStr {
    fn eq(&self, other: &Self) -> bool {
        **self == **other
    }
}

impl Default for PdStr {
    fn default() -> Self {
        Self {
            len: 0,
            data: "".as_ptr() as *mut c_void,
        }
    }
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn test_pd_str() {
        let s = "one_two_three";
        let pd = PdStr::from(s);
        assert_eq!(pd.deref(), "one_two_three");

        let s = String::from("one_two");
        let pd = PdStr::from(&s);
        assert_eq!(pd.deref(), "one_two");
        assert_eq!(&*pd, "one_two");
    }
}
