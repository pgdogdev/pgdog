//! Handle COPY.

use csv::ReaderBuilder;
use pg_query::{protobuf::CopyStmt, NodeEnum};
use pgdog_plugin::bindings::*;

use crate::sharding_function::bigint;

/// Parse COPY statement.
pub fn parse(stmt: &CopyStmt) -> Result<Copy, pg_query::Error> {
    if !stmt.is_from {
        return Ok(Copy::invalid());
    }

    if let Some(ref rel) = stmt.relation {
        let mut headers = false;
        let mut csv = false;

        for option in &stmt.options {
            if let Some(NodeEnum::DefElem(ref elem)) = option.node {
                match elem.defname.to_lowercase().as_str() {
                    "format" => {
                        if let Some(ref arg) = elem.arg {
                            if let Some(NodeEnum::String(ref string)) = arg.node {
                                match string.sval.to_lowercase().as_str() {
                                    "csv" => {
                                        csv = true;
                                    }

                                    _ => (),
                                }
                            }
                        }
                    }

                    "header" => {
                        headers = true;
                    }

                    _ => (),
                }
            }
        }

        if csv {
            return Ok(Copy::new(&rel.relname, headers));
        }
    }

    Ok(Copy::invalid())
}

/// Split copy data into individual rows
/// and determine where each row should go.
pub fn copy_data(input: CopyInput) -> Result<CopyOutput, csv::Error> {
    let data = input.data();
    let mut csv = ReaderBuilder::new()
        .has_headers(input.headers != 0)
        .from_reader(data);

    let mut rows = vec![];

    while let Some(record) = csv.records().next() {
        let record = record?;
        if let Some(position) = record.position() {
            let start = position.byte() as usize;
            let end = start + record.as_slice().len();
            let row_data = &data[start..=end]; // =end because of new line that's truncated

            let key = record.iter().skip(input.sharding_column as usize).next();
            let shard = key
                .map(|k| {
                    k.parse::<i64>()
                        .ok()
                        .map(|k| bigint(k, input.num_shards as usize) as i64)
                })
                .flatten()
                .unwrap_or(-1);

            let row = CopyRow::new(row_data, shard as i32);
            rows.push(row);
        }
    }

    Ok(CopyOutput::new(&rows).with_header(if csv.has_headers() {
        csv.headers()
            .ok()
            .map(|s| s.into_iter().map(|s| s).collect::<Vec<_>>().join(","))
    } else {
        None
    }))
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn test_copy_data() {
        let data = "id,email\n1,test@test.com\n2,admin@test.com\n";
        let input = CopyInput::new(data.as_bytes(), 0, 4, true);
        let output = copy_data(input).unwrap();

        let mut rows = output.rows().into_iter();
        assert_eq!(rows.next().unwrap().shard, bigint(1, 4) as i32);
        assert_eq!(rows.next().unwrap().shard, bigint(2, 4) as i32);
        assert_eq!(output.header(), Some("id,email"));
    }
}
