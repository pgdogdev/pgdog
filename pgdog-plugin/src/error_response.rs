//! ErrorResponse message.
use std::ffi::{CStr, CString};
use std::ptr;

use crate::bindings::PdErrorResponse;

/// Error response, returned to the client upon blocking a query
/// from executing.
#[derive(Debug, Default, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub struct ErrorResponse {
    /// Severity, e.g., INFO, WARNING, ERROR, etc.
    pub severity: String,
    /// Error code. See <https://www.postgresql.org/docs/current/errcodes-appendix.html> for list of well-known error codes.
    pub code: String,
    /// Error message as it will be shown to the client.
    pub message: String,
    /// Additional error details (optional).
    pub detail: Option<String>,
    /// Error context (optional).
    pub context: Option<String>,
    /// The file where the error occured. Helps with debugging. Optional.
    pub file: Option<String>,
    /// The name of the function that returned the error. Helps with debugging. Optional.
    pub routine: Option<String>,
}

impl PdErrorResponse {
    /// Create a NULL ErrorResponse, i.e., no error.
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
    /// Error response not set (is null).
    pub fn is_none(&self) -> bool {
        self.severity.is_null()
    }

    /// Take ownership of the error response and deallocate its memory.
    /// This consumes the PdErrorResponse and properly deallocates all C strings.
    pub unsafe fn deallocate(&mut self) {
        fn deallocate_cstring_ptr(ptr: &mut *const std::os::raw::c_char) {
            unsafe {
                if !ptr.is_null() {
                    let _ = CString::from_raw(*ptr as *mut std::os::raw::c_char);
                }
            }

            *ptr = ptr::null();
        }

        deallocate_cstring_ptr(&mut self.severity);
        deallocate_cstring_ptr(&mut self.code);
        deallocate_cstring_ptr(&mut self.message);
        deallocate_cstring_ptr(&mut self.detail);
        deallocate_cstring_ptr(&mut self.context);
        deallocate_cstring_ptr(&mut self.file);
        deallocate_cstring_ptr(&mut self.routine);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_pd_error_response_deallocate() {
        let error_response = ErrorResponse {
            severity: "ERROR".to_string(),
            code: "42P01".to_string(),
            message: "table does not exist".to_string(),
            detail: Some("The table 'users' was not found".to_string()),
            context: None,
            file: Some("executor.rs".to_string()),
            routine: Some("execute_query".to_string()),
        };

        let mut pd_error: PdErrorResponse = error_response.into();

        assert!(!pd_error.severity.is_null());
        assert!(!pd_error.code.is_null());
        assert!(!pd_error.message.is_null());
        assert!(!pd_error.detail.is_null());
        assert!(pd_error.context.is_null());
        assert!(!pd_error.file.is_null());
        assert!(!pd_error.routine.is_null());

        unsafe { pd_error.deallocate() };
    }

    #[test]
    fn test_pd_error_response_deallocate_none() {
        let mut pd_error = PdErrorResponse::none();
        assert!(pd_error.is_none());

        unsafe { pd_error.deallocate() };
    }
}
