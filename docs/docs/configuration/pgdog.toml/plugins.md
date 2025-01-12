# Plugin settings

[Plugins](../../features/plugins/index.md) are dynamically loaded at pooler startup. These settins control which plugins are loaded. In the future, more
options will be available to configure plugin behavior.

Plugins are a TOML list, so for each plugin you want to enable, add a `[[plugins]]` entry to `pgdog.toml`. For example:

```toml
[[plugins]]
name = "bob_router"

[[plugins]]
name = "alice_router"
```

### **`name`**

Name of the plugin to load. This is used by pgDog to look up the shared library object in [`LD_LIBRARY_PATH`](https://tldp.org/HOWTO/Program-Library-HOWTO/shared-libraries.html). For example, if your plugin
name is `router`, pgDog will look for `librouter.so` on Linux, `librouter.dll` on Windows and `librouter.dylib` on Mac OS.
