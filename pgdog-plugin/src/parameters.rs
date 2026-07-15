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
//! if let Some(param) = params.parameters.get(0) {
//!     let value = param.decode(params.parameter_format(0));
//! }
//! ```
use crate::ParameterFormat;
use std::{ptr, slice, str};

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

#[repr(C)]
#[derive(Debug, Clone, Copy)]
/// Prepared statement bound parameter.
pub struct Parameter<'a> {
    len: usize,
    /// The pointer to the data
    data: Option<&'a u8>,
}

impl<'a> Parameter<'a> {
    /// Construct this from the given slice
    pub fn new(data: Option<&'a [u8]>) -> Self {
        let len = data.map(|d| d.len()).unwrap_or(usize::MAX);
        let data = data.and_then(|d| d.get(0));
        Self { len, data }
    }

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
    /// let parameter = Parameter::new(None);
    /// assert!(parameter.decode(ParameterFormat::Text).is_none());
    ///
    /// let parameter = Parameter::new(Some(b"hello"));
    /// assert_eq!(parameter.decode(ParameterFormat::Text), Some(ParameterValue::Text("hello")));
    /// ```
    ///
    pub fn decode(&self, format: ParameterFormat) -> Option<ParameterValue<'_>> {
        match format {
            ParameterFormat::Binary => Some(ParameterValue::Binary(self.data()?)),
            ParameterFormat::Text => str::from_utf8(self.data()?).ok().map(ParameterValue::Text),
        }
    }

    /// The length of the slice, or usize::MAX for null
    pub fn len(&self) -> usize {
        self.len
    }

    /// Is the data empty? (Note that this is not the same as `null`, as some
    /// types are valid with no data)
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// The raw data for this parameter
    pub fn data(&self) -> Option<&[u8]> {
        if self.len == usize::MAX {
            None
        } else {
            // SAFETY: This struct is only constructed by `Parameters::from_raw`
            // which guarantees that this is safe. Neither `data` nor `len` are
            // public
            Some(unsafe {
                slice::from_raw_parts(self.data.map(ptr::from_ref).unwrap_or_default(), self.len)
            })
        }
    }

    /// Returns true if the parameter is `NULL`.
    pub fn is_null(&self) -> bool {
        self.len == usize::MAX
    }
}

impl<'a> From<Option<&'a [u8]>> for Parameter<'a> {
    fn from(value: Option<&'a [u8]>) -> Self {
        Self {
            len: value.map(|s| s.len()).unwrap_or(usize::MAX),
            data: value.and_then(|s| s.first()),
        }
    }
}

impl PartialEq for Parameter<'_> {
    fn eq(&self, other: &Self) -> bool {
        self.data() == other.data()
    }
}

impl Eq for Parameter<'_> {}

/// The FFI representation of [`Parameters`]
#[repr(C)]
pub struct RawParameters<'a> {
    parameters_data: Option<&'a Parameter<'a>>,
    parameters_len: usize,
    format_codes_data: Option<&'a ParameterFormat>,
    format_codes_len: usize,
}

/// Prepared statement parameters.
#[derive(Debug, PartialEq, Eq, Clone, Copy, Default)]
pub struct Parameters<'a> {
    pub parameters: &'a [Parameter<'a>],
    pub format_codes: &'a [ParameterFormat],
}

impl<'a> Parameters<'a> {
    /// Get a parameter format code indicating the encoding used.
    pub fn parameter_format(&self, param: usize) -> ParameterFormat {
        match self.format_codes.len() {
            0 => ParameterFormat::Text,
            1 => self.format_codes[0],
            _ => *self
                .format_codes
                .get(param)
                .unwrap_or(&ParameterFormat::Text),
        }
    }

    /// Convert this into an FFI-safe representation to be passed to
    /// [`Self::into_raw`] on the other side of the FFI boundary
    pub fn as_raw(&'a self) -> RawParameters<'a> {
        RawParameters {
            parameters_data: self.parameters.first(),
            parameters_len: self.parameters.len(),
            format_codes_data: self.format_codes.first(),
            format_codes_len: self.format_codes.len(),
        }
    }

    /// Construct this type from the FFI safe representation
    pub fn from_raw(raw: RawParameters<'a>) -> Self {
        // SAFETY: into_raw always returns a valid pointer/len combo
        let parameters = unsafe {
            slice::from_raw_parts(
                raw.parameters_data.map(ptr::from_ref).unwrap_or_default(),
                raw.parameters_len,
            )
        };
        // SAFETY: into_raw always returns a valid pointer/len combo
        let format_codes = unsafe {
            slice::from_raw_parts(
                raw.format_codes_data.map(ptr::from_ref).unwrap_or_default(),
                raw.format_codes_len,
            )
        };
        Self {
            parameters,
            format_codes,
        }
    }
}
