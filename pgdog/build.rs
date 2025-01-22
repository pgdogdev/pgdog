fn main() {
    println!("cargo:rerun-if-changed=src/frontend/router/sharding/hashfn.c");

    cc::Build::new()
        .file("src/frontend/router/sharding/hashfn.c")
        .compile("postgres_hash");
}
