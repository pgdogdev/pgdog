fn main() {
    // Detect the version of pg_query used by the plugin.
    // It must match whatever version PgDog is using and this
    // is checked when the plugin is loaded at runtime.
    pgdog_plugin_build::pg_query_version();
}
