use std::process::Command;

// Compile time checks.
fn main() {
    println!("cargo:rerun-if-changed=src/frontend/router/sharding/hashfn.c");

    cc::Build::new()
        .file("src/frontend/router/sharding/hashfn.c")
        .flags(["-Wno-implicit-fallthrough"])
        .compile("postgres_hash");

    println!("cargo:rerun-if-env-changed=GIT_HASH");

    let git_hash = std::env::var("GIT_HASH").unwrap_or_else(|_| {
        Command::new("git")
            .args(["rev-parse", "HEAD"])
            .output()
            .map(|output| {
                String::from_utf8(output.stdout)
                    .unwrap_or_default()
                    .chars()
                    .take(7)
                    .collect()
            })
            .unwrap_or_else(|_| env!("CARGO_PKG_VERSION").to_string())
    });

    println!("cargo:rustc-env=GIT_HASH={git_hash}");
}
