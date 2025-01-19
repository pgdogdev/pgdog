fn main() {
    cc::Build::new()
        .file("postgres_hash/hashfn.c")
        .compile("postgres_hash");
}
