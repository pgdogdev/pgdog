//! Plugin interface.
use std::ops::Deref;

use crate::bindings::{self, Input, Output};
use libloading::{library_filename, Library, Symbol};

/// Plugin interface.
#[derive(Debug)]
pub struct Plugin<'a> {
    name: String,
    /// Initialization routine.
    init: Option<Symbol<'a, unsafe extern "C" fn()>>,
    /// Shutdown routine.
    fini: Option<Symbol<'a, unsafe extern "C" fn()>>,
    /// Route query to a shard.
    route: Option<Symbol<'a, unsafe extern "C" fn(bindings::Input) -> Output>>,
}

impl<'a> Plugin<'a> {
    /// Load library using a cross-platform naming convention.
    pub fn library(name: &str) -> Result<Library, libloading::Error> {
        let name = library_filename(name);
        unsafe { Library::new(name) }
    }

    /// Load standard methods from the plugin library.
    pub fn load(name: &str, library: &'a Library) -> Self {
        let route = unsafe { library.get(b"pgdog_route_query\0") }.ok();
        let init = unsafe { library.get(b"pgdog_init\0") }.ok();
        let fini = unsafe { library.get(b"pgdog_fini\0") }.ok();

        Self {
            name: name.to_owned(),
            route,
            init,
            fini,
        }
    }

    /// Route query.
    pub fn route(&self, input: Input) -> Option<Output> {
        self.route.as_ref().map(|route| unsafe { route(input) })
    }

    /// Perform initialization.
    pub fn init(&self) -> bool {
        if let Some(init) = &self.init {
            unsafe {
                init();
            }
            true
        } else {
            false
        }
    }

    pub fn fini(&self) {
        if let Some(ref fini) = &self.fini {
            unsafe { fini() }
        }
    }

    /// Plugin name.
    pub fn name(&self) -> &str {
        &self.name
    }

    /// Check that we have the required methods.
    pub fn valid(&self) -> bool {
        self.route.is_some()
    }
}

pub struct PluginOutput {
    output: Output,
}

impl PluginOutput {
    pub fn new(output: Output) -> Self {
        Self { output }
    }
}

impl Deref for PluginOutput {
    type Target = Output;

    fn deref(&self) -> &Self::Target {
        &self.output
    }
}

impl Drop for PluginOutput {
    fn drop(&mut self) {
        unsafe {
            self.output.deallocate();
        }
    }
}

pub struct PluginInput {
    input: Input,
}

impl PluginInput {
    pub fn new(input: Input) -> Self {
        Self { input }
    }
}

impl Deref for PluginInput {
    type Target = Input;

    fn deref(&self) -> &Self::Target {
        &self.input
    }
}

impl Drop for PluginInput {
    fn drop(&mut self) {
        unsafe {
            self.input.deallocate();
        }
    }
}
