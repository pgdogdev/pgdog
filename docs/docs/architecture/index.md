# Architecture

pgDog is written in async Rust, using the Tokio runtime. This allows the pooler to take advantage of multiple
CPU cores, when available. [Plugins](../features/plugins/index.md) are written as shared libraries
and are loaded into the executable at runtime.
