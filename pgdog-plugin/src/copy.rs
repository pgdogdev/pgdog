use std::{ffi::c_void, slice::from_raw_parts};

use pg_query::{protobuf::CopyStmt, NodeEnum};

use crate::bindings::PdCopyRow;

/// Copy format.
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum Format {
    /// Text (can be CSV or Postgres text).
    Text,
    /// Binary COPY format.
    Binary,
}

impl PdCopyRow {
    /// Create a new `PdCopyRow` from a parsed COPY statement.
    ///
    /// The caller must ensure `copy` and `data` outlive the returned struct,
    /// since it holds raw pointers into both.
    pub fn from_proto(copy: &CopyStmt, shards: usize, data: &[u8]) -> Self {
        Self {
            shards: shards as u64,
            copy_stmt: copy as *const CopyStmt as *const c_void,
            data_len: data.len() as u64,
            data: data.as_ptr() as *const c_void,
        }
    }

    /// Get the CopyStmt protobuf.
    pub fn copy_stmt(&self) -> &CopyStmt {
        unsafe { &*(self.copy_stmt as *const CopyStmt) }
    }

    /// Helper to look up a string-valued option from the COPY statement.
    pub fn option(&self, name: &str) -> Option<&str> {
        for option in &self.copy_stmt().options {
            if let Some(NodeEnum::DefElem(ref elem)) = option.node {
                if elem.defname.eq_ignore_ascii_case(name) {
                    if let Some(ref arg) = elem.arg {
                        if let Some(NodeEnum::String(ref s)) = arg.node {
                            return Some(&s.sval);
                        }
                    }
                    // Option present but no value (e.g. HEADER).
                    return Some("");
                }
            }
        }
        None
    }

    /// Check if a boolean-style option is present (e.g. HEADER).
    fn has_option(&self, name: &str) -> bool {
        self.option(name).is_some()
    }

    /// Get column names from the COPY statement.
    pub fn columns(&self) -> Vec<&str> {
        self.copy_stmt()
            .attlist
            .iter()
            .filter_map(|node| {
                if let Some(NodeEnum::String(ref s)) = node.node {
                    Some(s.sval.as_str())
                } else {
                    None
                }
            })
            .collect()
    }

    /// Get number of shards.
    pub fn shards(&self) -> u64 {
        self.shards
    }

    /// Get raw data. Caller is responsible for decoding.
    /// The data will contain exactly one row.
    pub fn data(&self) -> &[u8] {
        unsafe { from_raw_parts(self.data as *const u8, self.data_len as usize) }
    }

    /// Get row format.
    pub fn format(&self) -> Format {
        match self.option("format") {
            Some(f) if f.eq_ignore_ascii_case("binary") => Format::Binary,
            _ => Format::Text,
        }
    }

    /// Get delimiter.
    pub fn delimiter(&self) -> char {
        if let Some(d) = self.option("delimiter") {
            return d.chars().next().unwrap_or(',');
        }

        // CSV defaults to comma, text/binary default to tab.
        match self.option("format") {
            Some(f) if f.eq_ignore_ascii_case("csv") => ',',
            _ => '\t',
        }
    }

    /// Get NULL string.
    pub fn null_string(&self) -> &str {
        self.option("null").unwrap_or("\\N")
    }

    /// Whether the COPY includes headers.
    pub fn headers(&self) -> bool {
        // Binary format always has a header.
        self.has_option("header") || self.format() == Format::Binary
    }
}

#[cfg(test)]
mod test {
    use pg_query::{parse, NodeEnum};

    use super::*;

    fn parse_copy(sql: &str) -> CopyStmt {
        let parsed = parse(sql).unwrap();
        let stmt = parsed.protobuf.stmts.first().unwrap();
        match stmt.stmt.clone().unwrap().node.unwrap() {
            NodeEnum::CopyStmt(copy) => *copy,
            _ => panic!("not a COPY statement"),
        }
    }

    #[test]
    fn test_text_defaults() {
        let copy = parse_copy("COPY t (id, value) FROM STDIN");
        let data = b"1\thello\n";
        let row = PdCopyRow::from_proto(&copy, 3, data);

        assert_eq!(row.shards(), 3);
        assert_eq!(row.format(), Format::Text);
        assert_eq!(row.delimiter(), '\t');
        assert_eq!(row.null_string(), "\\N");
        assert!(!row.headers());
        assert_eq!(row.data(), data.as_slice());
        assert_eq!(row.columns(), vec!["id", "value"]);
    }

    #[test]
    fn test_csv_defaults() {
        let copy = parse_copy("COPY t (a, b, c) FROM STDIN CSV");
        let row = PdCopyRow::from_proto(&copy, 2, b"1,2,3\n");

        assert_eq!(row.format(), Format::Text);
        assert_eq!(row.delimiter(), ',');
        assert!(!row.headers());
        assert_eq!(row.columns(), vec!["a", "b", "c"]);
    }

    #[test]
    fn test_csv_header() {
        let copy = parse_copy("COPY t (x) FROM STDIN CSV HEADER");
        let row = PdCopyRow::from_proto(&copy, 1, b"x\n1\n");

        assert!(row.headers());
        assert_eq!(row.delimiter(), ',');
    }

    #[test]
    fn test_custom_delimiter() {
        let copy = parse_copy("COPY t FROM STDIN CSV DELIMITER '|'");
        let row = PdCopyRow::from_proto(&copy, 1, b"a|b\n");

        assert_eq!(row.delimiter(), '|');
    }

    #[test]
    fn test_custom_null() {
        let copy = parse_copy("COPY t (id) FROM STDIN CSV NULL 'NULL'");
        let row = PdCopyRow::from_proto(&copy, 1, b"NULL\n");

        assert_eq!(row.null_string(), "NULL");
    }

    #[test]
    fn test_binary_format() {
        let copy = parse_copy("COPY t FROM STDIN (FORMAT 'binary')");
        let row = PdCopyRow::from_proto(&copy, 4, b"\x00");

        assert_eq!(row.format(), Format::Binary);
        assert!(row.headers());
        assert_eq!(row.shards(), 4);
    }

    #[test]
    fn test_no_columns() {
        let copy = parse_copy("COPY t FROM STDIN");
        let row = PdCopyRow::from_proto(&copy, 1, b"1\t2\n");

        assert!(row.columns().is_empty());
    }

    #[test]
    fn test_explicit_text_format() {
        let copy = parse_copy(r#"COPY "public"."t" ("id", "val") FROM STDIN WITH (FORMAT text)"#);
        let row = PdCopyRow::from_proto(&copy, 2, b"1\thello\n");

        assert_eq!(row.format(), Format::Text);
        assert_eq!(row.delimiter(), '\t');
        assert_eq!(row.columns(), vec!["id", "val"]);
    }
}
