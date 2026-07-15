#[macro_export]
/// Generates the required scaffolding for PgDog to load this plugin at
/// runtime.
///
/// This macro should be called with the name of a type that implements [`crate::Plugin`].
macro_rules! plugin {
    ($plugin_type: ident) => {
        // SAFETY: This static is never assigned, only read
        #[unsafe(no_mangle)]
        pub static PGDOG_PLUGIN_VTABLE: $crate::plugin::PluginVtable =
            $crate::plugin::PluginVtable::from_plugin::<$plugin_type>();
    };
}
