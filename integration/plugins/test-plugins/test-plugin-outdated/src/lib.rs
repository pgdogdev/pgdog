//! Tests that the plugin that uses outdated version of pgdog-plugin is skipped.
//! If the plugin is loaded, the test will fail because of the panic.

use pgdog_plugin::{macros, Context, Route};

macros::plugin!();

#[macros::route]
fn route(_context: Context) -> Route {
    panic!("The outdated plugin should not be loaded");
}
