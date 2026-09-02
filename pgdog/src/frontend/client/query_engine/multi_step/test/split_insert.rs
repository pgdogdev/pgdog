// Note: Most of these tests previously were in parser/rewrite/statement/insert.rs

use pg_raw_parse::Node;

use crate::frontend::client::query_engine::multi_step::types::StepRequest;
use crate::{
    frontend::{
        ClientRequest,
        client::{
            query_engine::{
                QueryEngineContext,
                multi_step::{
                    error::Error,
                    insert::create_steps,
                    types::{QueryPlanner, ResponseHistory},
                },
            },
            test::TestClient,
        },
        router::parser::rewrite::statement::Error as RewriteError,
    },
    net::{
        Bind, Execute, Parameters, Parse, Sync,
        messages::bind::{Format, Parameter},
    },
};

#[derive(Debug)]
struct InsertSplit {
    params: Vec<u16>,
    stmt: String,
}

impl InsertSplit {
    fn extract_bind_params(&self, bind: &Bind) -> Result<Bind, Error> {
        QueryPlanner::rewrite_bind(&self.params.iter().copied().collect(), bind, "")
    }
}

fn placeholders(sql: &str) -> u16 {
    sql.split('$')
        .skip(1)
        .filter_map(|rest| {
            rest.chars()
                .take_while(|c| c.is_ascii_digit())
                .collect::<String>()
                .parse::<u16>()
                .ok()
        })
        .max()
        .unwrap_or_default()
}

async fn parse_and_split(sql: &str) -> Vec<InsertSplit> {
    let root = pg_raw_parse::parse(sql).unwrap();
    let insert = match root.stmts().next() {
        Some(Node::InsertStmt(insert)) => insert,
        _ => unreachable!(),
    };

    let params = (1..=placeholders(sql))
        .map(|number| Parameter::new(number.to_string().as_bytes()))
        .collect::<Vec<_>>();
    let request = ClientRequest::from(vec![
        Parse::new_anonymous(sql).into(),
        Bind::new_params("", &params).into(),
        Execute::new().into(),
        Sync.into(),
    ]);

    let mut client = TestClient::new_rewrites(Parameters::default()).await;
    client.client.client_request = request.clone();
    let mut context = QueryEngineContext::new(&mut client.client);

    let steps = create_steps(insert, &client.engine, &mut context, &request).unwrap();

    steps
        .into_iter()
        .map(|step| {
            let StepRequest::Statement(statement) = step.request else {
                unreachable!("split should not be raw")
            };
            let request = statement
                .assemble(&ResponseHistory::default())
                .unwrap()
                .expect("split resolves statically");
            let stmt = request.query().unwrap().unwrap().query().to_string();
            let params = request
                .parameters()
                .unwrap()
                .map(|bind| {
                    bind.params_raw()
                        .iter()
                        .map(|param| {
                            std::str::from_utf8(&param.data)
                                .unwrap()
                                .parse::<u16>()
                                .unwrap()
                        })
                        .collect()
                })
                .unwrap_or_default();
            InsertSplit { params, stmt }
        })
        .collect()
}

#[tokio::test]
async fn test_split_insert_with_params() {
    let splits =
        parse_and_split("INSERT INTO my_table (id, value) VALUES ($1, $2), ($3, $4)").await;

    assert_eq!(splits.len(), 2);

    // First tuple uses params 0 and 1 (original $1, $2)
    assert_eq!(splits[0].params.as_slice(), &[1, 2]);
    assert_eq!(
        splits[0].stmt,
        "INSERT INTO my_table (id, value) VALUES ($1, $2)"
    );

    // Second tuple uses params 2 and 3 (original $3, $4), renumbered to $1, $2
    assert_eq!(splits[1].params.as_slice(), &[3, 4]);
    assert_eq!(
        splits[1].stmt,
        "INSERT INTO my_table (id, value) VALUES ($1, $2)"
    );
}

#[tokio::test]
async fn test_split_insert_single_tuple_no_split() {
    let splits = parse_and_split("INSERT INTO my_table (id, value) VALUES ($1, $2)").await;

    // Single tuple should not be split
    assert!(splits.is_empty());
}

#[tokio::test]
async fn test_split_insert_literal_values() {
    let splits =
        parse_and_split("INSERT INTO my_table (id, value) VALUES (1, 'a'), (2, 'b')").await;

    assert_eq!(splits.len(), 2);

    // No params for literal values
    assert!(splits[0].params.is_empty());
    assert_eq!(
        splits[0].stmt,
        "INSERT INTO my_table (id, value) VALUES (1, 'a')"
    );

    assert!(splits[1].params.is_empty());
    assert_eq!(
        splits[1].stmt,
        "INSERT INTO my_table (id, value) VALUES (2, 'b')"
    );
}

#[tokio::test]
async fn test_split_insert_mixed_params_and_literals() {
    let splits =
        parse_and_split("INSERT INTO my_table (id, value) VALUES ($1, 'a'), ($2, 'b')").await;

    assert_eq!(splits.len(), 2);

    assert_eq!(splits[0].params.as_slice(), &[1]);
    assert_eq!(
        splits[0].stmt,
        "INSERT INTO my_table (id, value) VALUES ($1, 'a')"
    );

    assert_eq!(splits[1].params.as_slice(), &[2]);
    assert_eq!(
        splits[1].stmt,
        "INSERT INTO my_table (id, value) VALUES ($1, 'b')"
    );
}

#[tokio::test]
async fn test_extract_bind_params() {
    let splits = parse_and_split("INSERT INTO t (a, b) VALUES ($1, $2), ($3, $4)").await;
    let bind = Bind::new_params(
        "test",
        &[
            Parameter::new(b"p0"),
            Parameter::new(b"p1"),
            Parameter::new(b"p2"),
            Parameter::new(b"p3"),
        ],
    );

    // First split uses params 0 and 1
    let extracted = splits[0].extract_bind_params(&bind).unwrap();
    assert_eq!(extracted.params_raw().len(), 2);
    assert_eq!(extracted.params_raw()[0].data.as_ref(), b"p0");
    assert_eq!(extracted.params_raw()[1].data.as_ref(), b"p1");

    // Second split uses params 2 and 3
    let extracted = splits[1].extract_bind_params(&bind).unwrap();
    assert_eq!(extracted.params_raw().len(), 2);
    assert_eq!(extracted.params_raw()[0].data.as_ref(), b"p2");
    assert_eq!(extracted.params_raw()[1].data.as_ref(), b"p3");
}

#[tokio::test]
async fn test_extract_bind_params_with_format_codes() {
    let splits = parse_and_split("INSERT INTO t (a, b) VALUES ($1, $2), ($3, $4)").await;
    let bind = Bind::new_params_codes(
        "test",
        &[
            Parameter::new(b"p0"),
            Parameter::new(b"p1"),
            Parameter::new(b"p2"),
            Parameter::new(b"p3"),
        ],
        &[Format::Text, Format::Binary, Format::Text, Format::Binary],
    );

    // Second split uses params 2 and 3 (Text, Binary)
    let extracted = splits[1].extract_bind_params(&bind).unwrap();
    assert_eq!(extracted.params_raw().len(), 2);
    assert_eq!(extracted.params_raw()[0].data.as_ref(), b"p2");
    assert_eq!(extracted.params_raw()[1].data.as_ref(), b"p3");
    assert_eq!(extracted.format_codes_raw().len(), 2);
    assert_eq!(extracted.format_codes_raw()[0], Format::Text);
    assert_eq!(extracted.format_codes_raw()[1], Format::Binary);
}

#[tokio::test]
async fn test_extract_bind_params_uniform_format() {
    let splits = parse_and_split("INSERT INTO t (a) VALUES ($1), ($2)").await;
    let bind = Bind::new_params_codes(
        "test",
        &[Parameter::new(b"p0"), Parameter::new(b"p1")],
        &[Format::Binary], // Uniform format
    );

    let extracted = splits[0].extract_bind_params(&bind).unwrap();
    assert_eq!(extracted.params_raw().len(), 1);
    assert_eq!(extracted.format_codes_raw().len(), 1);
    assert_eq!(extracted.format_codes_raw()[0], Format::Binary);
}

#[tokio::test]
async fn test_extract_bind_params_mixed_params_and_literals() {
    let splits = parse_and_split("INSERT INTO t (a, b) VALUES ($1, 'lit1'), ($2, 'lit2')").await;
    let bind = Bind::new_params(
        "test",
        &[
            Parameter::new(b"value_for_param1"),
            Parameter::new(b"value_for_param2"),
        ],
    );

    assert_eq!(splits.len(), 2);

    // First split: statement uses $1 with literal, bind extracts param 0
    assert_eq!(splits[0].stmt, "INSERT INTO t (a, b) VALUES ($1, 'lit1')");
    let extracted = splits[0].extract_bind_params(&bind).unwrap();
    assert_eq!(extracted.params_raw().len(), 1);
    assert_eq!(extracted.params_raw()[0].data.as_ref(), b"value_for_param1");

    // Second split: statement uses $1 (renumbered from $2) with literal, bind extracts param 1
    assert_eq!(splits[1].stmt, "INSERT INTO t (a, b) VALUES ($1, 'lit2')");
    let extracted = splits[1].extract_bind_params(&bind).unwrap();
    assert_eq!(extracted.params_raw().len(), 1);
    assert_eq!(extracted.params_raw()[0].data.as_ref(), b"value_for_param2");
}

#[tokio::test]
async fn test_extract_bind_params_varying_param_counts() {
    // First tuple has 2 params, second tuple has 1 param and 1 literal
    let splits = parse_and_split("INSERT INTO t (a, b) VALUES ($1, $2), ($3, 'literal')").await;
    let bind = Bind::new_params(
        "test",
        &[
            Parameter::new(b"p1"),
            Parameter::new(b"p2"),
            Parameter::new(b"p3"),
        ],
    );

    assert_eq!(splits.len(), 2);

    // First split: uses params 0 and 1 (original $1, $2)
    assert_eq!(splits[0].stmt, "INSERT INTO t (a, b) VALUES ($1, $2)");
    let extracted = splits[0].extract_bind_params(&bind).unwrap();
    assert_eq!(extracted.params_raw().len(), 2);
    assert_eq!(extracted.params_raw()[0].data.as_ref(), b"p1");
    assert_eq!(extracted.params_raw()[1].data.as_ref(), b"p2");

    // Second split: uses param 2 (original $3), renumbered to $1
    assert_eq!(
        splits[1].stmt,
        "INSERT INTO t (a, b) VALUES ($1, 'literal')"
    );
    let extracted = splits[1].extract_bind_params(&bind).unwrap();
    assert_eq!(extracted.params_raw().len(), 1);
    assert_eq!(extracted.params_raw()[0].data.as_ref(), b"p3");
}

#[tokio::test]
async fn test_extract_bind_params_incorrect_count() {
    let splits = parse_and_split("INSERT INTO t (a, b) VALUES ($1, $2), ($3, $4)").await;
    let bind = Bind::new_params(
        "test",
        &[
            Parameter::new(b"p1"),
            Parameter::new(b"p2"),
            Parameter::new(b"p3"),
        ],
    );

    std::assert_matches!(splits[0].extract_bind_params(&bind), Ok(_));
    std::assert_matches!(
        splits[1].extract_bind_params(&bind),
        Err(Error::Rewrite(RewriteError::MissingParameter(_)))
    );
}
