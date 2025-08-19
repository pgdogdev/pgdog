//! Prepared statement parameters.
//!
//! # Example
//!
//! ```
//! use pgdog_plugin::prelude::*;
//!
//! let params = Parameters::default();
//! assert_eq!(ParameterFormat::Text, params.parameter_format(0));
//!
//! if let Some(param) = params.get(0) {
//!     let value = param.decode(params.parameter_format(0));
//! }
//! ```
use std::{ops::Deref, os::raw::c_void, ptr::null, str::from_utf8};

use crate::PdParameters;

/// Parameter format code. 0 is text encoding (usually UTF-8), 1 is binary encoding,
/// specific to the parameter data type.
#[derive(Debug, Clone, PartialEq, Eq)]
#[repr(C)]
pub enum ParameterFormat {
    /// Text encoding.
    Text = 0,

    /// Binary encoding.
    Binary = 1,
}

/// Wrapper around a decoded parameter.
///
/// # Example
///
/// ```
/// # use pgdog_plugin::prelude::*;
/// let parameter = ParameterValue::Text("test");
/// match parameter {
///     ParameterValue::Text(text) => assert_eq!(text, "test"),
///     ParameterValue::Binary(binary) => println!("{:?}", binary),
/// }
/// ```
#[derive(Debug, PartialEq, Eq)]
pub enum ParameterValue<'a> {
    /// Parameter is encoded using text (UTF-8).
    Text(&'a str),
    /// Parameter is encoded using binary encoding.
    Binary(&'a [u8]),
}

/// Prepared statement bound parameter.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Parameter {
    /// Parameter data length.
    pub len: i32,
    /// Parameter data.
    ///
    /// Use [`Self::decode`] to read this value.
    pub data: Vec<u8>,
}

impl Parameter {
    /// Decode parameter given the provided format. If the parameter is encoded using text encoding (default),
    /// a UTF-8 string is returned. If encoded using binary encoding, a slice of bytes.
    ///
    /// If the parameter is `NULL` or the encoding doesn't match the data, `None` is returned.
    ///
    /// # Example
    ///
    /// ```
    /// use pgdog_plugin::prelude::*;
    ///
    /// let parameter = Parameter {
    ///     len: -1,
    ///     data: vec![],
    /// };
    /// assert!(parameter.decode(ParameterFormat::Text).is_none());
    ///
    /// let parameter = Parameter {
    ///     len: 5,
    ///     data: "hello".as_bytes().to_vec(),
    /// };
    /// assert_eq!(parameter.decode(ParameterFormat::Text), Some(ParameterValue::Text("hello")));
    /// ```
    ///
    pub fn decode(&self, format: ParameterFormat) -> Option<ParameterValue<'_>> {
        if self.len == -1 {
            return None;
        }
        match format {
            ParameterFormat::Binary => Some(ParameterValue::Binary(&self.data[..])),
            ParameterFormat::Text => from_utf8(&self.data).ok().map(ParameterValue::Text),
        }
    }

    /// Returns true if the parameter is `NULL`.
    pub fn null(&self) -> bool {
        self.len == -1
    }
}

/// Prepared statement parameters.
#[derive(Debug, PartialEq, Eq)]
pub struct Parameters {
    params: Option<Vec<Parameter>>,
    format_codes: Option<Vec<ParameterFormat>>,
    borrowed: bool,
}

impl Default for Parameters {
    fn default() -> Self {
        Self {
            params: Some(vec![]),
            format_codes: Some(vec![]),
            borrowed: false,
        }
    }
}

impl Clone for Parameters {
    fn clone(&self) -> Self {
        Self {
            params: self.params.clone(),
            format_codes: self.format_codes.clone(),
            borrowed: false,
        }
    }
}

impl Parameters {
    /// Returns a list of parameter format codes.
    pub fn format_codes(&self) -> &[ParameterFormat] {
        self.format_codes.as_ref().unwrap()
    }

    /// Get a parameter format code indicating the encoding used.
    pub fn parameter_format(&self, param: usize) -> ParameterFormat {
        match self.format_codes().len() {
            0 => ParameterFormat::Text,
            1 => self.format_codes()[0].clone(),
            _ => self
                .format_codes()
                .get(param)
                .unwrap_or(&ParameterFormat::Text)
                .clone(),
        }
    }
}

impl Deref for Parameters {
    type Target = Vec<Parameter>;

    fn deref(&self) -> &Self::Target {
        self.params.as_ref().unwrap()
    }
}

impl From<PdParameters> for Parameters {
    fn from(value: PdParameters) -> Self {
        if value.format_codes.is_null() || value.params.is_null() {
            return Self {
                params: Some(vec![]),
                format_codes: Some(vec![]),
                borrowed: false,
            };
        }

        // SAFETY: The parameters have the same data type, size and alignment because it's created
        // by `Vec::new`.
        //
        // This is enforced by the Rust compiler and plugins are required to use the same version
        // as PgDog.
        //
        // Why not use a slice instead? This vec (and others in this crate) typically contain
        // other heap-allocated data types, like other vecs
        //
        // We don't implement `DerefMut`, so this vec is read-only. The `*mut Parameter`
        // cast doesn't do anything.
        //
        // We use `std::mem::forget` below so this vec doesn't actually own this
        // data.
        //
        // Cloning the vec is safe: the `Clone::clone` method uses `len` only and
        // creates a new vec with that as its capacity. It then iterates over each
        // element and calls its `Clone::clone` function.
        //
        // Since this data is immutable and we are not actually taking ownership, this
        // call is safe.
        let params = unsafe {
            Vec::from_raw_parts(
                value.params as *mut Parameter,
                value.num_params as usize,
                value.num_params as usize,
            )
        };

        // SAFETY: Same note as above.
        let format_codes = unsafe {
            Vec::from_raw_parts(
                value.format_codes as *mut ParameterFormat,
                value.num_format_codes as usize,
                value.num_format_codes as usize,
            )
        };

        Self {
            params: Some(params),
            format_codes: Some(format_codes),
            borrowed: true,
        }
    }
}

impl Drop for Parameters {
    fn drop(&mut self) {
        if self.borrowed {
            std::mem::forget(self.params.take());
            std::mem::forget(self.format_codes.take());
        }
    }
}

impl Default for PdParameters {
    fn default() -> Self {
        Self {
            num_params: 0,
            params: null::<c_void>() as *mut c_void,
            num_format_codes: 0,
            format_codes: null::<c_void>() as *mut c_void,
        }
    }
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn test_empty_params() {
        let params = PdParameters::default();
        let params: Parameters = params.into();

        println!("{:?}", params);
    }
}
