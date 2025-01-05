UNAME := $(shell uname)

all:
	gcc -L"../../target/debug" -lpgdog_plugin plugin.c -shared -o librouting_plugin_c.so
	cp librouting_plugin_c.so ../../target/debug
	cp librouting_plugin_c.so ../../target/debug/librouting_plugin_c.dylib

release:
	gcc -L"../../target/release" -lpgdog_plugin plugin.c -O2 -shared -o librouting_plugin_c.so
	cp librouting_plugin_c.so ../../target/release
	cp librouting_plugin_c.so ../../target/release/librouting_plugin_c.dylib
