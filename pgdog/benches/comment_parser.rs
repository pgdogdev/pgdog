use brunch::{Bench, benches};
use pgdog::frontend::router::parser::comment::parse_edge_comment;

const QUERY_WITH_LEADING: &str =
    "/* pgdog_shard: 5 */ SELECT * FROM users WHERE id = $1 AND name = $2";
const QUERY_WITH_TRAILING: &str =
    "SELECT * FROM users WHERE id = $1 AND name = $2 /* pgdog_role: primary */";
const QUERY_NO_COMMENT: &str = "SELECT * FROM users WHERE id = $1 AND name = $2";

benches!(
    Bench::new("parse_edge_comment(leading)")
        .run(|| parse_edge_comment(QUERY_WITH_LEADING, &Default::default())),
    Bench::new("parse_edge_comment(trailing)")
        .run(|| parse_edge_comment(QUERY_WITH_TRAILING, &Default::default())),
    Bench::new("parse_edge_comment(no comment)")
        .run(|| parse_edge_comment(QUERY_NO_COMMENT, &Default::default())),
);
