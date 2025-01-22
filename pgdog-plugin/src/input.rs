//! Plugin input helpers.
#![allow(non_upper_case_globals)]
use crate::bindings::{self, *};

impl bindings::Input {
    /// Create new plugin input.
    pub fn new_query(config: Config, input: RoutingInput) -> Self {
        Self {
            config,
            input,
            input_type: InputType_ROUTING_INPUT,
        }
    }

    pub fn new_copy(config: Config, input: RoutingInput) -> Self {
        Self {
            config,
            input,
            input_type: InputType_COPY_INPUT,
        }
    }

    /// Deallocate memory.
    ///
    /// # Safety
    ///
    /// This is not to be used by plugins.
    /// # Safety
    ///
    /// This is for internal pgDog usage only.
    pub unsafe fn deallocate(&self) {
        self.config.deallocate();
    }

    /// Get query if this is a routing input.
    pub fn query(&self) -> Option<bindings::Query> {
        match self.input_type {
            InputType_ROUTING_INPUT => Some(unsafe { self.input.query }),
            _ => None,
        }
    }

    /// Get copy input, if any.
    pub fn copy(&self) -> Option<CopyInput> {
        if self.input_type == InputType_COPY_INPUT {
            Some(unsafe { self.input.copy })
        } else {
            None
        }
    }
}

impl RoutingInput {
    /// Create query routing input.
    pub fn query(query: bindings::Query) -> Self {
        Self { query }
    }

    /// Create copy routing input.
    pub fn copy(copy: CopyInput) -> Self {
        Self { copy }
    }
}
