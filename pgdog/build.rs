use std::process::Command;

fn main() {
    println!("cargo:rerun-if-changed=src/frontend/router/sharding/hashfn.c");

    cc::Build::new()
        .file("src/frontend/router/sharding/hashfn.c")
        .compile("postgres_hash");

    let output = Command::new("git").args(["rev-parse", "HEAD"]).output();
    if let Ok(output) = output {
        let git_hash = String::from_utf8(output.stdout).unwrap_or_default();
        println!(
            "cargo:rustc-env=GIT_HASH={}",
            git_hash.chars().take(7).collect::<String>()
        );
    } else {
        println!("cargo:rustc-env=GIT_HASH={}", env!("CARGO_PKG_VERSION"));
    }
}
