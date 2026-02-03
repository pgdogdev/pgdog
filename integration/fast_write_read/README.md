# Race condition search

1. `cd` into this directory.
2. In psql, run `CREATE DATABASE pgdog`, `CREATE USER pgdog PASSWORD 'pgdog' LOGIN` (if not exists already).
3. `cargo run --release --bin pgdog` (release is faster, helps find race condtions).
4. In a different terminal, run `npm start`.
