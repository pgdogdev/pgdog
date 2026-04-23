use brunch::{benches, Bench};
use pg_query::scan_raw;
use pgdog::backend::ShardingSchema;
use pgdog::frontend::router::parser::comment::{comment, parse_edge_comment};

const QUERY_WITH_LEADING: &str =
    "/* pgdog_shard: 5 */ SELECT * FROM users WHERE id = $1 AND name = $2";
const QUERY_WITH_TRAILING: &str =
    "SELECT * FROM users WHERE id = $1 AND name = $2 /* pgdog_role: primary */";
const QUERY_NO_COMMENT: &str = "SELECT * FROM users WHERE id = $1 AND name = $2";

benches!(
    Bench::new("parse_edge_comment(leading)")
        .run(|| parse_edge_comment(QUERY_WITH_LEADING, &ShardingSchema::default())),
    Bench::new("parse_edge_comment(trailing)")
        .run(|| parse_edge_comment(QUERY_WITH_TRAILING, &ShardingSchema::default())),
    Bench::new("parse_edge_comment(no comment)")
        .run(|| parse_edge_comment(QUERY_NO_COMMENT, &ShardingSchema::default())),
    Bench::new("scan_raw(leading)").run(|| scan_raw(QUERY_WITH_LEADING)),
    Bench::new("scan_raw(trailing)").run(|| scan_raw(QUERY_WITH_TRAILING)),
    Bench::new("scan_raw(no comment)").run(|| scan_raw(QUERY_NO_COMMENT)),
    Bench::new("comment() via scan_raw (leading)")
        .run(|| comment(QUERY_WITH_LEADING, &ShardingSchema::default())),
    Bench::new("comment() via scan_raw (trailing)")
        .run(|| comment(QUERY_WITH_TRAILING, &ShardingSchema::default())),
    Bench::new("comment() via scan_raw (no comment)")
        .run(|| comment(QUERY_NO_COMMENT, &ShardingSchema::default())),
);
