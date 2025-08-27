use super::super::CopyFormat;
use std::{ops::Range, str::from_utf8};

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
                            println!("escaping");
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
    pub(super) fn new(
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
