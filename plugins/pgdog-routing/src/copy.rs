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
        let mut delimiter = ',';

        let mut columns = vec![];

        for column in &stmt.attlist {
            if let Some(NodeEnum::String(ref column)) = column.node {
                columns.push(column.sval.as_str());
            }
        }

        for option in &stmt.options {
            if let Some(NodeEnum::DefElem(ref elem)) = option.node {
                match elem.defname.to_lowercase().as_str() {
                    "format" => {
                        if let Some(ref arg) = elem.arg {
                            if let Some(NodeEnum::String(ref string)) = arg.node {
                                if string.sval.to_lowercase().as_str() == "csv" {
                                    csv = true;
                                }
                            }
                        }
                    }

                    "delimiter" => {
                        if let Some(ref arg) = elem.arg {
                            if let Some(NodeEnum::String(ref string)) = arg.node {
                                delimiter = string.sval.chars().next().unwrap_or(',');
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
            return Ok(Copy::new(&rel.relname, headers, delimiter, &columns));
        }
    }

    Ok(Copy::invalid())
}

/// Split copy data into individual rows
/// and determine where each row should go.
pub fn copy_data(input: CopyInput, shards: usize) -> Result<CopyOutput, csv::Error> {
    let data = input.data();
    let mut csv = ReaderBuilder::new()
        .has_headers(input.headers())
        .delimiter(input.delimiter() as u8)
        .from_reader(data);

    let mut rows = vec![];

    while let Some(record) = csv.records().next() {
        let record = record?;
        if let Some(position) = record.position() {
            let start = position.byte() as usize;
            let end = start + record.as_slice().len();
            // N.B.: includes \n character which indicates the end of a single CSV record.
            // If CSV is encoded using Windows \r\n, this will break.
            if let Some(row_data) = data.get(start..=end + 1) {
                let key = record.iter().nth(input.sharding_column());
                let shard = key
                    .and_then(|k| k.parse::<i64>().ok().map(|k| bigint(k, shards) as i64))
                    .unwrap_or(-1);

                let row = CopyRow::new(row_data, shard as i32);
                rows.push(row);
            }
        }
    }

    Ok(CopyOutput::new(&rows).with_header(if csv.has_headers() {
        csv.headers().ok().map(|s| {
            s.into_iter()
                .collect::<Vec<_>>()
                .join(input.delimiter().to_string().as_str())
                + "\n" // New line indicating the end of a CSV line.
        })
    } else {
        None
    }))
}

#[cfg(test)]
mod test {

    use super::*;

    #[test]
    fn test_copy() {
        let stmt = "COPY test_table FROM 'some_file.csv' CSV HEADER DELIMITER ';'";
        let ast = pg_query::parse(stmt).unwrap();
        let copy = ast.protobuf.stmts.first().unwrap().stmt.clone().unwrap();

        let copy = match copy.node {
            Some(NodeEnum::CopyStmt(ref stmt)) => parse(stmt).unwrap(),
            _ => panic!("not COPY"),
        };

        assert_eq!(copy.copy_format, CopyFormat_CSV);
        assert_eq!(copy.delimiter(), ';');
        assert!(copy.has_headers());
        assert_eq!(copy.table_name(), "test_table");

        let data = "id;email\n1;test@test.com\n2;admin@test.com\n";
        let input = CopyInput::new(data.as_bytes(), 0, copy.has_headers(), ';');
        let output = copy_data(input, 4).unwrap();

        let mut rows = output.rows().iter();
        assert_eq!(rows.next().unwrap().shard, bigint(1, 4) as i32);
        assert_eq!(rows.next().unwrap().shard, bigint(2, 4) as i32);
        assert_eq!(output.header(), Some("id;email\n"));

        unsafe {
            copy.deallocate();
            output.deallocate();
        }
    }
}
