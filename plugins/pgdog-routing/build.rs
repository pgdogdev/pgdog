fn main() {
    println!("cargo:rerun-if-changed=postgres_hash/hashfn.c");
    cc::Build::new()
        .file("postgres_hash/hashfn.c")
        .compile("postgres_hash");
}
