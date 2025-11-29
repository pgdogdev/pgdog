//! Rewrite module interface.

use super::{Error, Input};

/// Rewrite trait.
///
/// All rewrite modules should follow this.
pub trait RewriteModule {
    /// Take a statement and maybe rewrite it, if needed.
    ///
    /// If a rewrite is needed, the module should mutate the statement
    /// and update the Bind message.
    fn rewrite(&mut self, input: &mut Input<'_>) -> Result<(), Error>;
}
