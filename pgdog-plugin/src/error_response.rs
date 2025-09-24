use std::ffi::{CStr, CString};
use std::ptr;

use crate::bindings::PdErrorResponse;

#[derive(Debug, Default, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub struct ErrorResponse {
    pub severity: String,
    pub code: String,
    pub message: String,
    pub detail: Option<String>,
    pub context: Option<String>,
    pub file: Option<String>,
    pub routine: Option<String>,
}

impl PdErrorResponse {
    pub fn none() -> Self {
        let null = ptr::null();
        Self {
            severity: null,
            code: null,
            message: null,
            detail: null,
            context: null,
            file: null,
            routine: null,
        }
    }
}

impl From<PdErrorResponse> for ErrorResponse {
    fn from(value: PdErrorResponse) -> Self {
        fn cstr_to_string(ptr: *const std::os::raw::c_char) -> String {
            if ptr.is_null() {
                String::new()
            } else {
                unsafe { CStr::from_ptr(ptr).to_string_lossy().to_string() }
            }
        }

        fn cstr_to_option_string(ptr: *const std::os::raw::c_char) -> Option<String> {
            if ptr.is_null() {
                None
            } else {
                unsafe { Some(CStr::from_ptr(ptr).to_string_lossy().to_string()) }
            }
        }

        Self {
            severity: cstr_to_string(value.severity),
            code: cstr_to_string(value.code),
            message: cstr_to_string(value.message),
            detail: cstr_to_option_string(value.detail),
            context: cstr_to_option_string(value.context),
            file: cstr_to_option_string(value.file),
            routine: cstr_to_option_string(value.routine),
        }
    }
}

impl From<ErrorResponse> for PdErrorResponse {
    fn from(value: ErrorResponse) -> Self {
        fn string_to_ptr(s: String) -> *const std::os::raw::c_char {
            CString::new(s).unwrap().into_raw()
        }

        fn option_string_to_ptr(s: Option<String>) -> *const std::os::raw::c_char {
            match s {
                Some(s) => string_to_ptr(s),
                None => ptr::null(),
            }
        }

        Self {
            severity: string_to_ptr(value.severity),
            code: string_to_ptr(value.code),
            message: string_to_ptr(value.message),
            detail: option_string_to_ptr(value.detail),
            context: option_string_to_ptr(value.context),
            file: option_string_to_ptr(value.file),
            routine: option_string_to_ptr(value.routine),
        }
    }
}

impl PdErrorResponse {
    pub fn is_none(&self) -> bool {
        self.severity.is_null()
    }
}
