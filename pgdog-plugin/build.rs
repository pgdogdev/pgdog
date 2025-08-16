use std::{path::PathBuf, process::Command};

fn main() {
    println!("cargo:rerun-if-changed=include/types.h");

    let bindings = bindgen::Builder::default()
        .header("include/wrapper.h")
        .generate_comments(true)
        // Tell cargo to invalidate the built crate whenever any of the
        // included header files changed.
        .parse_callbacks(Box::new(bindgen::CargoCallbacks::new()))
        // Finish the builder and generate the bindings.
        .generate()
        // Unwrap the Result and panic on failure.
        .expect("Unable to generate bindings");

    let out_path = PathBuf::from("src");
    let _ = bindings.write_to_file(out_path.join("bindings.rs"));

    let rustc = std::env::var("RUSTC").unwrap();
    let version = Command::new(rustc).arg("--version").output().unwrap();
    let version_str = String::from_utf8(version.stdout).unwrap();

    println!("cargo:rustc-env=RUSTC_VERSION={}", version_str.trim());
}
