use std::process::Command;

#[cfg(not(docsrs))]
fn main() {
    let rustc = std::env::var("RUSTC").unwrap();
    let version = Command::new(rustc).arg("--version").output().unwrap();
    let version_str = String::from_utf8(version.stdout).unwrap();

    println!("cargo:rustc-env=RUSTC_VERSION={}", version_str.trim());
}
