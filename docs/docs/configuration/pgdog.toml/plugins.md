# Plugin settings

[Plugins](../../features/plugins/index.md) are dynamically loaded at pooler startup. These settings control which plugins are loaded. In the future, more
options will be available to configure plugin behavior.

Plugins are a TOML list, so for each plugin you want to enable, add a `[[plugins]]` entry to `pgdog.toml`. For example:

```toml
[[plugins]]
name = "bob_router"

[[plugins]]
name = "alice_router"
```

!!! note
    Plugins can only be configured at PgDog startup. They cannot be changed after
    the process is running.

### **`name`**

Name of the plugin to load. This is used by PgDog to look up the shared library object in [`LD_LIBRARY_PATH`](https://tldp.org/HOWTO/Program-Library-HOWTO/shared-libraries.html). For example, if your plugin
name is `router`, PgDog will look for `librouter.so` on Linux, `librouter.dll` on Windows and `librouter.dylib` on Mac OS.
