# Plugins in C

Writing PgDog plugins in C is pretty straight forward if you're comfortable in the language. The plugin API
is written in C (for compatibility), so if you're comfortable in C, you should be right at home.

## Getting started

### Includes

The plugin headers are located in `pgdog-plugin/include`. Include `pgdog.h` for everything you need to get started:

```c
#include "pgdog.h"
```

### Linking

Your plugin will use `pgdog-plugin` internals, so you need to link to it at build time. To do so, first compile
`pgdog-plugin` by running this command in the root directory of the project:

```bash
cargo build
```

This ensures all libraries and bindings are compiled before you get started.

!!! note
    If you're writing plugins for release (`-02`), build the crate using the release profile by passing `--release` flag to Cargo.

The shared library will be placed in `target/(debug|release)` and you can link to it like so:

```bash
export LIBRARY_PATH=target/debug
gcc plugin.c -lpgdog_routing -lshared -o plugin.so
```

### Memory safety

All structures passed to plugins are owned by PgDog runtime, so make sure not to `free` any pointers. All structures passed back to PgDog will be freed automatically by PgDog, so you don't need to worry about leaks.

If you allocate any memory during routine execution, make sure to free it before you return from the plugin API call.

### Globals

Access to `pgdog_route_query` is _not_ synchronized, so if you use any globals, make sure they are static or
protected by a mutex. You can initialize any globals in `pgdog_init` and clean them up in `pgdog_fini`.

## Learn more

- [routing-plugin-c](https://github.com/levkk/pgdog/tree/main/examples/routing-plugin-c) example plugin

See [Rust](rust.md) documentation for how to implement plugins.
