use std::{ffi::c_void, ops::Range, str::from_utf8};

use crate::bindings::{PdCopyRow, PdStr};

#[derive(Debug, Clone, Copy, PartialEq)]
pub enum CopyFormat {
    Text,
    Csv,
    Binary,
}

/// A complete CSV record.
#[derive(Clone)]
pub struct Record {
    /// Raw record data.
    pub data: Vec<u8>,
    /// Field ranges.
    pub fields: Vec<Range<usize>>,
    /// Delimiter.
    pub delimiter: char,
    /// Format used.
    pub format: CopyFormat,
    /// Null string.
    pub null_string: String,
}

impl std::fmt::Debug for Record {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Record")
            .field("data", &from_utf8(&self.data))
            .field("fields", &self.fields)
            .field("delimiter", &self.delimiter)
            .field("format", &self.format)
            .field("null_string", &self.null_string)
            .finish()
    }
}

impl std::fmt::Display for Record {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        writeln!(
            f,
            "{}",
            (0..self.len())
                .map(|field| match self.format {
                    CopyFormat::Csv => {
                        let text = self.get(field).unwrap();
                        if text == self.null_string {
                            text.to_owned()
                        } else {
                            format!("\"{}\"", self.get(field).unwrap().replace("\"", "\"\""))
                        }
                    }
                    _ => self.get(field).unwrap().to_string(),
                })
                .collect::<Vec<String>>()
                .join(&format!("{}", self.delimiter))
        )
    }
}

impl Record {
    pub fn new(
        data: &[u8],
        ends: &[usize],
        delimiter: char,
        format: CopyFormat,
        null_string: &str,
    ) -> Self {
        let mut last = 0;
        let mut fields = vec![];
        for e in ends {
            fields.push(last..*e);
            last = *e;
        }
        Self {
            data: data.to_vec(),
            fields,
            delimiter,
            format,
            null_string: null_string.to_owned(),
        }
    }

    /// Number of fields in the record.
    pub fn len(&self) -> usize {
        self.fields.len()
    }

    /// Return true if there are no fields in the record.
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    pub fn get(&self, index: usize) -> Option<&str> {
        self.fields
            .get(index)
            .cloned()
            .and_then(|range| from_utf8(&self.data[range]).ok())
    }
}

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
    pub fn from_proto(
        shards: usize,
        record: &Record,
        column_names: &[PdStr],
        table_name: &PdStr,
        schema_name: &PdStr,
    ) -> Self {
        Self {
            shards: shards as u64,
            record: record as *const Record as *const c_void,
            num_columns: column_names.len() as u64,
            columns: column_names.as_ptr() as *mut PdStr,
            table_name: table_name as *const PdStr as *mut PdStr,
            schema_name: schema_name as *const PdStr as *mut PdStr,
        }
    }

    /// Get number of shards.
    pub fn shards(&self) -> u64 {
        self.shards
    }

    /// Get the parsed record.
    pub fn record(&self) -> &Record {
        unsafe { &*(self.record as *const Record) }
    }

    /// Get column names.
    pub fn columns(&self) -> Vec<&str> {
        if self.num_columns == 0 {
            return vec![];
        }
        unsafe {
            std::slice::from_raw_parts(self.columns, self.num_columns as usize)
                .iter()
                .map(|s| &**s)
                .collect()
        }
    }

    /// Get table name.
    pub fn table_name(&self) -> &str {
        if self.table_name.is_null() {
            return "";
        }
        unsafe { &*self.table_name }
    }

    /// Get schema name.
    pub fn schema_name(&self) -> &str {
        if self.schema_name.is_null() {
            return "";
        }
        unsafe { &*self.schema_name }
    }
}
