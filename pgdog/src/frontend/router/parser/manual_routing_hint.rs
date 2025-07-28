// -------------------------------------------------------------------------------------------------
// ----- README ------------------------------------------------------------------------------------
//
// This module implements manual routing hints for PgDog via comments and CTEs.
//
// WARNING:
// - This is not production-ready.
// - The idea is to have this for all manual routing hint detection go through one function.
// - (e.g., SET pgdog.shard or SET pgdog.sharding_key).
// - QueryParser can call this module and store the results in the QueryParser struct.
//
// UNCERTAIN:
// - Should we widen then Regex rules for sharding_key?
// - I added `_` to the old Regex which wasn't supported... but seems like any character should be
//   allowed? I believe the example below would currently fail the Regex match.
//
//     ex: let video_id = nanoid();       // %_a8^a&!#@3g
//
// *** UNCERTAINS SHOULD BE HANDLED BEFORE MERGING TO TRUNK ***
//
// -------------------------------------------------------------------------------------------------

use once_cell::sync::Lazy;
use pg_query::protobuf::a_const::Val::{Ival, Sval};
use pg_query::protobuf::{AConst, Integer, ParamRef};
use pg_query::NodeEnum;
use pg_query::{protobuf::Token, scan, Node, ParseResult};
use regex::Regex;

use crate::net::messages::Bind;

// -------------------------------------------------------------------------------------------------
// ----- Types -------------------------------------------------------------------------------------

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ManualRoutingHint {
    Shard(u32),
    ShardKey(ShardKeyValue),
    Conflict,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ShardKeyValue {
    Int(u64),
    Uuid(uuid::Uuid),
    Text(String),
}

// -------------------------------------------------------------------------------------------------
// ----- Constants ---------------------------------------------------------------------------------

static SHARD_COMMENT_REGEX: Lazy<Regex> =
    Lazy::new(|| Regex::new(r#"pgdog_shard: *([0-9]+)"#).unwrap());

static SHARDING_KEY_COMMENT_REGEX: Lazy<Regex> =
    Lazy::new(|| Regex::new(r#"pgdog_sharding_key: *([0-9a-zA-Z_\-]+)"#).unwrap());

// -------------------------------------------------------------------------------------------------
// ----- Public functions --------------------------------------------------------------------------

pub struct ManualRouting;

impl ManualRouting {
    pub fn find_hint(
        query_ast: &ParseResult,
        query_str: &str,
        binds: Option<&Bind>,
    ) -> Option<ManualRoutingHint> {
        let comment_hint = find_comment_hint(query_str);
        let cte_hint = find_cte_hint(query_ast, binds);

        match (comment_hint, cte_hint) {
            (None, None) => None,
            (Some(h), None) => Some(h),
            (None, Some(h)) => Some(h),
            (Some(_), Some(_)) => Some(ManualRoutingHint::Conflict),
        }
    }
}

// -------------------------------------------------------------------------------------------------
// ----- Manual Routing :: Comment -----------------------------------------------------------------

#[inline(always)]
fn find_comment_hint(query: &str) -> Option<ManualRoutingHint> {
    let mut hint: Option<ManualRoutingHint> = None;
    let mut hints_found = 0;

    let tokens = scan(query).ok()?.tokens;

    for token in tokens.iter() {
        if token.token != Token::CComment as i32 {
            continue;
        }

        let comment = &query[token.start as usize..token.end as usize];

        if let Some(new_hint) = find_commented_shard_key(comment) {
            hint = Some(new_hint);
            hints_found += 1;
        }

        if let Some(new_hint) = find_commented_sharding_key(comment) {
            hint = Some(new_hint);
            hints_found += 1;
        }

        if hints_found > 1 {
            return Some(ManualRoutingHint::Conflict);
        }
    }

    hint
}

/// Try to pull out a `pgdog_shard:` value and turn it into a hint.
#[inline(always)]
fn find_commented_shard_key(comment: &str) -> Option<ManualRoutingHint> {
    let mut caps = SHARD_COMMENT_REGEX.captures_iter(comment);

    let first = match caps.next() {
        Some(c) => c,
        None => return None,
    };

    // Query contains more than than one shard key
    if caps.next().is_some() {
        return Some(ManualRoutingHint::Conflict);
    }

    let raw = match first.get(1) {
        Some(m) => m.as_str(),
        None => return None,
    };

    let shard_id = raw.parse::<u32>().ok()?;
    let hint = ManualRoutingHint::Shard(shard_id);

    Some(hint)
}

/// Try to pull out a `pgdog_sharding_key:` value and turn it into a hint.
#[inline(always)]
fn find_commented_sharding_key(comment: &str) -> Option<ManualRoutingHint> {
    let mut captures = SHARDING_KEY_COMMENT_REGEX.captures_iter(comment);

    let first = match captures.next() {
        Some(c) => c,
        None => return None,
    };

    // Query contains more than than one shard key
    if captures.next().is_some() {
        return Some(ManualRoutingHint::Conflict);
    }

    let raw = match first.get(1) {
        Some(m) => m.as_str(),
        None => return None,
    };

    let key = if let Ok(i) = raw.parse::<u64>() {
        ShardKeyValue::Int(i)
    } else if let Ok(u) = raw.parse::<uuid::Uuid>() {
        ShardKeyValue::Uuid(u)
    } else {
        ShardKeyValue::Text(raw.to_string())
    };

    let hint = ManualRoutingHint::ShardKey(key);

    Some(hint)
}

// -------------------------------------------------------------------------------------------------
// ----- Manual Routing :: Comment :: Tests --------------------------------------------------------

#[cfg(test)]
mod comment_tests {

    use super::*;

    #[test]
    fn shard_numeric() {
        let q = "/* pgdog_shard: 42 */ SELECT * FROM users;";

        let result = find_comment_hint(q);
        let msh = ManualRoutingHint::Shard(42);

        assert_eq!(result, Some(msh));
    }

    #[test]
    fn sharding_key_text() {
        let q = "/* pgdog_sharding_key: user_123 */ SELECT 1;";

        let result = find_comment_hint(q);
        let msh = ManualRoutingHint::ShardKey(ShardKeyValue::Text("user_123".into()));

        assert_eq!(result, Some(msh));
    }

    #[test]
    fn multiple_shard_conflict() {
        let q = "/* pgdog_shard: 1 */ /* pgdog_shard: 2 */ SELECT 1;";

        let result = find_comment_hint(q);
        let msh = ManualRoutingHint::Conflict;

        assert_eq!(result, Some(msh));
    }

    #[test]
    fn multiple_sharding_key_conflict() {
        let q = "/* pgdog_sharding_key: user_123 */ /* pgdog_sharding_key: user_456 */ SELECT 1;";

        let result = find_comment_hint(q);
        let msh = ManualRoutingHint::Conflict;

        assert_eq!(result, Some(msh));
    }

    #[test]
    fn shard_and_sharding_key_conflict() {
        let q = "/* pgdog_shard: 1 */ /* pgdog_sharding_key: user_123 */ SELECT 1;";

        let result = find_comment_hint(q);
        let msh = ManualRoutingHint::Conflict;

        assert_eq!(result, Some(msh));
    }
}

// -------------------------------------------------------------------------------------------------
// ----- Manual Routing :: CTE ---------------------------------------------------------------------

fn find_cte_hint(ast: &ParseResult, bind: Option<&Bind>) -> Option<ManualRoutingHint> {
    let has_override_cte = ast.cte_names.iter().any(|cte| cte == "pgdog_overrides");
    if !has_override_cte {
        return None;
    }

    let with_clause = {
        let first_stmt = ast.protobuf.stmts.first()?.stmt.as_ref()?;

        match first_stmt.node.as_ref()? {
            NodeEnum::SelectStmt(sel) => sel.with_clause.as_ref(),
            NodeEnum::InsertStmt(ins) => ins.with_clause.as_ref(),
            NodeEnum::UpdateStmt(upd) => upd.with_clause.as_ref(),
            NodeEnum::DeleteStmt(del) => del.with_clause.as_ref(),
            _ => return None,
        }?
    };

    let Some(first_cte) = with_clause.ctes.first() else {
        return None;
    };

    let Some(NodeEnum::CommonTableExpr(cte)) = &first_cte.node else {
        return None;
    };

    if cte.ctename != "pgdog_overrides" {
        return None;
    }

    let cte_select = match cte.ctequery.as_ref()?.node.as_ref()? {
        NodeEnum::SelectStmt(s) => s,
        _ => return None,
    };

    let mut shard: Option<u32> = None;
    let mut sharding_key: Option<ShardKeyValue> = None;

    for target in &cte_select.target_list {
        let node = match &target.node {
            Some(rt) => rt,
            None => continue,
        };

        let res_target = match node {
            NodeEnum::ResTarget(rt) => rt,
            _ => continue,
        };

        let name = res_target.name.as_str();
        let expression = &res_target.val;

        if name == "shard" {
            shard = extract_shard_value(expression, bind);
            continue;
        }

        if name == "sharding_key" {
            sharding_key = extract_sharding_key_value(expression, bind);
            continue;
        }
    }

    match (shard, sharding_key) {
        (Some(i), None) => Some(ManualRoutingHint::Shard(i)),
        (None, Some(k)) => Some(ManualRoutingHint::ShardKey(k)),
        _ => Some(ManualRoutingHint::Conflict),
    }
}

#[inline(always)]
fn extract_shard_value(expression: &Option<Box<Node>>, bind: Option<&Bind>) -> Option<u32> {
    let node = expression.as_ref()?.node.as_ref()?;

    // Strip top-level CAST wrapper and try again. ie: `SELECT 12::integer as shard;`
    if let NodeEnum::TypeCast(cast) = node {
        return extract_shard_value(&cast.arg, bind);
    }

    // literal
    if let NodeEnum::AConst(AConst {
        val: Some(Ival(Integer { ival })),
        ..
    }) = node
    {
        return Some(*ival as u32);
    }

    // bind
    if let NodeEnum::ParamRef(ParamRef { number, .. }) = node {
        return parse_shard_param(*number, bind);
    }

    None
}

#[inline(always)]
fn extract_sharding_key_value(
    expression: &Option<Box<Node>>,
    bind: Option<&Bind>,
) -> Option<ShardKeyValue> {
    let node = expression.as_ref()?.node.as_ref()?;

    // Strip top-level CAST wrapper and try again.
    if let NodeEnum::TypeCast(cast) = node {
        return extract_sharding_key_value(&cast.arg, bind);
    }

    // literal
    if let NodeEnum::AConst(AConst {
        val: Some(Sval(sv)),
        ..
    }) = node
    {
        if let Ok(u) = uuid::Uuid::parse_str(&sv.sval) {
            return Some(ShardKeyValue::Uuid(u));
        }
        return Some(ShardKeyValue::Text(sv.sval.clone()));
    }

    // bind
    if let NodeEnum::ParamRef(ParamRef { number, .. }) = node {
        return parse_sharding_key_param(*number, bind);
    }

    None
}

#[inline(always)]
fn parse_shard_param(param_number: i32, bind: Option<&Bind>) -> Option<u32> {
    if param_number <= 0 {
        return None;
    }

    let idx = (param_number as usize) - 1;
    let wrapper = bind?.parameter(idx).ok()??;
    wrapper.bigint().map(|v| v as u32)
}

#[inline(always)]
fn parse_sharding_key_param(param_number: i32, bind: Option<&Bind>) -> Option<ShardKeyValue> {
    if param_number <= 0 {
        return None;
    }

    let idx = (param_number as usize) - 1;
    let wrapper = bind?.parameter(idx).ok()??;

    // bigint
    if let Some(v) = wrapper.bigint() {
        return Some(ShardKeyValue::Int(v as u64));
    }

    let Some(text) = wrapper.text() else {
        return None;
    };

    // uuid
    if let Ok(u) = uuid::Uuid::parse_str(text) {
        return Some(ShardKeyValue::Uuid(u));
    }

    // text
    return Some(ShardKeyValue::Text(text.to_string()));
}

// -------------------------------------------------------------------------------------------------
// ----- Manual Routing :: CTE :: Tests ------------------------------------------------------------

#[cfg(test)]
mod cte_test {

    use super::*;
    use crate::net::messages::{Bind, Parameter};
    use pg_query::parse;
    use uuid::Uuid;

    fn ast(sql: &str) -> ParseResult {
        parse(sql).unwrap()
    }

    #[test]
    fn shard_cast_literal() {
        let sql = "WITH pgdog_overrides AS (SELECT 5::integer AS shard) SELECT 1;";
        let sql = ast(sql);

        let result = find_cte_hint(&sql, None);
        let mrh = ManualRoutingHint::Shard(5);

        assert_eq!(result, Some(mrh));
    }

    #[test]
    fn sharding_key_cast_literal() {
        let sql = "WITH pgdog_overrides AS (SELECT 'k1'::text AS sharding_key) SELECT 1;";
        let sql = ast(sql);

        let result = find_cte_hint(&sql, None);
        let mrh = ManualRoutingHint::ShardKey(ShardKeyValue::Text("k1".into()));

        assert_eq!(result, Some(mrh));
    }

    #[test]
    fn shard_cast_bind() {
        let sql = "WITH pgdog_overrides AS (SELECT $1::integer AS shard) SELECT 1;";
        let sql = ast(sql);

        let params = vec![Parameter {
            len: 2,
            data: b"99".to_vec(),
        }];
        let bind = Bind::test_params("", &params);

        let result = find_cte_hint(&sql, Some(&bind));
        let mrh = ManualRoutingHint::Shard(99);

        assert_eq!(result, Some(mrh));
    }

    #[test]
    fn sharding_key_uuid_cast_bind() {
        let uuid = "550e8400-e29b-41d4-a716-446655440000";

        let sql = "WITH pgdog_overrides AS (SELECT $1::uuid AS sharding_key) SELECT 1;";
        let sql = ast(sql);

        let params = vec![Parameter {
            len: 36,
            data: Uuid::parse_str(uuid).unwrap().to_string().into_bytes(),
        }];
        let bind = Bind::test_params("", &params);

        let result = find_cte_hint(&sql, Some(&bind));
        let mrh = ManualRoutingHint::ShardKey(ShardKeyValue::Uuid(Uuid::parse_str(uuid).unwrap()));

        assert_eq!(result, Some(mrh));
    }

    #[test]
    fn shard_and_key_conflict() {
        let sql = r#"

            WITH
            pgdog_overrides AS (
                SELECT
                    7::integer AS shard,
                    'abc'::text AS sharding_key
            )
            SELECT 1;

        "#;
        let sql = ast(sql);

        let result = find_cte_hint(&sql, None);
        let mrh = ManualRoutingHint::Conflict;

        assert_eq!(result, Some(mrh));
    }

    #[test]
    fn mutliple_ctes_litteral() {
        let sql = r#"

            WITH
            pgdog_overrides AS (
                SELECT 12::integer AS shard
            ),
            recent_messages AS (
                SELECT
                    *
                FROM
                    messages
                WHERE
                    messages.user_id = $2::uuid
                    AND messages.sent_at >= NOW() - INTERVAL '24 hours'
            )
            SELECT
                COUNT(*) AS total_messages
            FROM
                recent_messages;

        "#;
        let sql = ast(sql);

        let user_id = "550e8400-e29b-41d4-a716-446655440000";
        let user_id_param = Parameter {
            len: 36,
            data: Uuid::parse_str(user_id).unwrap().to_string().into_bytes(),
        };
        let params = vec![user_id_param];
        let bind = Bind::test_params("", &params);

        let result = find_cte_hint(&sql, Some(&bind));
        let mrh = ManualRoutingHint::Shard(12);

        assert_eq!(result, Some(mrh));
    }

    #[test]
    fn mutliple_ctes_with_bind() {
        let user_id = "550e8400-e29b-41d4-a716-446655440000";

        let sql = r#"

            WITH
            pgdog_overrides AS (
                SELECT $1::integer AS shard
            ),
            recent_messages AS (
                SELECT
                    *
                FROM
                    messages
                WHERE
                    messages.user_id = $2::uuid
                    AND messages.sent_at >= NOW() - INTERVAL '24 hours'
            )
            SELECT
                COUNT(*) AS total_messages
            FROM
                recent_messages;

        "#;
        let sql = ast(sql);

        let params = vec![
            Parameter {
                len: 2,
                data: b"12".to_vec(),
            },
            Parameter {
                len: 36,
                data: Uuid::parse_str(user_id).unwrap().to_string().into_bytes(),
            },
        ];
        let bind = Bind::test_params("", &params);

        let result = find_cte_hint(&sql, Some(&bind));
        let mrh = ManualRoutingHint::Shard(12);

        assert_eq!(result, Some(mrh));
    }
}

// -------------------------------------------------------------------------------------------------
// ----- Tests ~ Multi -----------------------------------------------------------------------------

#[cfg(test)]
mod mutli_tests {

    use super::*;
    use pg_query::parse;

    fn ast(sql: &str) -> ParseResult {
        parse(sql).unwrap()
    }

    #[test]
    fn comment_shard_vs_cte_shard_conflict() {
        let query_str = r#"

            /* pgdog_shard: 1 */

            WITH pgdog_overrides AS (
                SELECT 2::integer AS shard
            )
            SELECT 1;

        "#;

        let query_ast = ast(query_str);

        let result = ManualRouting::find_hint(&query_ast, query_str, None);
        let mrh = ManualRoutingHint::Conflict;

        assert_eq!(result, Some(mrh));
    }

    #[test]
    fn comment_key_vs_cte_key_conflict() {
        let query_str = r#"

            /* pgdog_sharding_key: user_123 */

            WITH pgdog_overrides AS (
                SELECT 'user_456'::text AS sharding_key
            )
            SELECT 1;
        "#;

        let query_ast = ast(query_str);

        let result = ManualRouting::find_hint(&query_ast, query_str, None);
        let mrh = ManualRoutingHint::Conflict;

        assert_eq!(result, Some(mrh));
    }

    #[test]
    fn comment_shard_vs_cte_key_conflict() {
        let query_str = r#"

            /* pgdog_shard: 3 */

            WITH
            pgdog_overrides AS (
                SELECT 'user_123'::text AS sharding_key
            )
            SELECT 1;

        "#;

        let query_ast = ast(query_str);

        let result = ManualRouting::find_hint(&query_ast, query_str, None);
        let mrh = ManualRoutingHint::Conflict;

        assert_eq!(result, Some(mrh));
    }
}

// -------------------------------------------------------------------------------------------------
// -------------------------------------------------------------------------------------------------
