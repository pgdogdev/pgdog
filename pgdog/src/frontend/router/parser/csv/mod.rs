use csv_core::{ReadRecordResult, Reader, ReaderBuilder};

pub mod iterator;
pub mod record;

pub(crate) use iterator::Iter;
pub(crate) use record::Record;

use super::CopyFormat;

static RECORD_BUFFER: usize = 4096;
static ENDS_BUFFER: usize = 2048; // Max of 2048 columns in a CSV.
                                  // Postgres supports a max of 1600 columns in a table,
                                  // so we are well within bounds.

/// CSV reader that can handle partial inputs.
#[derive(Clone)]
pub(crate) struct CsvStream {
    /// Input buffer.
    buffer: Vec<u8>,
    /// Temporary buffer for the record.
    record: Vec<u8>,
    /// Temporary buffer for indices for the fields in a record.
    ends: Vec<usize>,
    /// CSV reader.
    reader: Reader,
    /// Number of bytes read so far.
    read: usize,
    /// CSV deliminter.
    delimiter: char,
    /// Null string.
    null_string: String,
    /// First record are headers.
    headers: bool,
    /// Read headers.
    headers_record: Option<Record>,
    /// Copy format
    format: CopyFormat,
}

impl std::fmt::Debug for CsvStream {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("CsvStream")
            .field("read", &self.read)
            .field("delimiter", &self.delimiter)
            .field("headers", &self.headers)
            .field("format", &self.format)
            .finish()
    }
}

impl CsvStream {
    /// Create new CSV stream reader.
    pub(crate) fn new(
        delimiter: char,
        headers: bool,
        format: CopyFormat,
        null_string: &str,
    ) -> Self {
        Self {
            buffer: Vec::new(),
            record: vec![0u8; RECORD_BUFFER],
            ends: vec![0usize; ENDS_BUFFER],
            reader: Self::reader(delimiter),
            read: 0,
            null_string: null_string.to_owned(),
            delimiter,
            headers,
            headers_record: None,
            format,
        }
    }

    fn reader(delimiter: char) -> Reader {
        ReaderBuilder::new()
            .delimiter(delimiter as u8)
            .double_quote(true)
            .build()
    }

    /// Write some data to the CSV stream.
    ///
    /// This data will be appended to the input buffer. To read records from
    /// that stream, call [`Self::record`].
    pub(crate) fn write(&mut self, data: &[u8]) {
        self.buffer.extend(data);
    }

    /// Fetch a record from the stream. This mutates the inner buffer,
    /// so you can only fetch the record once.
    pub(crate) fn record(&mut self) -> Result<Option<Record>, super::Error> {
        loop {
            let (result, read, written, ends) = self.reader.read_record(
                &self.buffer[self.read..],
                &mut self.record,
                &mut self.ends,
            );

            match result {
                ReadRecordResult::OutputFull => {
                    self.record.resize(self.buffer.len() * 2 + 1, 0u8);
                    self.reader = Self::reader(self.delimiter);
                }

                // Data incomplete.
                ReadRecordResult::InputEmpty | ReadRecordResult::End => {
                    self.buffer = Vec::from(&self.buffer[self.read..]);
                    self.read = 0;
                    self.reader = Self::reader(self.delimiter);
                    return Ok(None);
                }

                ReadRecordResult::Record => {
                    let record = Record::new(
                        &self.record[..written],
                        &self.ends[..ends],
                        self.delimiter,
                        self.format,
                        &self.null_string,
                    );
                    self.read += read;
                    self.record.fill(0u8);

                    if self.headers && self.headers_record.is_none() {
                        self.headers_record = Some(record);
                        return Ok(None);
                    } else {
                        return Ok(Some(record));
                    }
                }

                ReadRecordResult::OutputEndsFull => {
                    return Err(super::Error::MaxCsvParserRows);
                }
            }
        }
    }

    /// Get an iterator over all records available in the buffer.
    pub(crate) fn records(&mut self) -> Iter<'_> {
        Iter::new(self)
    }

    /// Get headers from the CSV, if any.
    pub(crate) fn headers(&mut self) -> Result<Option<&Record>, super::Error> {
        if self.headers {
            if let Some(ref headers) = self.headers_record {
                return Ok(Some(headers));
            } else {
                self.record()?;
                if let Some(ref headers) = self.headers_record {
                    return Ok(Some(headers));
                }
            }
        }

        Ok(None)
    }
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn test_csv_stream() {
        let csv = "one,two,three\nfour,five,six\nseven,eight";
        let mut reader = CsvStream::new(',', false, CopyFormat::Csv, "\\N");
        reader.write(csv.as_bytes());

        let record = reader.record().unwrap().unwrap();
        assert_eq!(record.get(0), Some("one"));
        assert_eq!(record.get(1), Some("two"));
        assert_eq!(record.get(2), Some("three"));

        let record = reader.record().unwrap().unwrap();
        assert_eq!(record.get(0), Some("four"));
        assert_eq!(record.get(1), Some("five"));
        assert_eq!(record.get(2), Some("six"));

        assert!(reader.record().unwrap().is_none());

        reader.write(",nine\n".as_bytes());

        let record = reader.record().unwrap().unwrap();
        assert_eq!(record.get(0), Some("seven"));
        assert_eq!(record.get(1), Some("eight"));
        assert_eq!(record.get(2), Some("nine"));
        assert_eq!(record.to_string(), "\"seven\",\"eight\",\"nine\"\n");

        assert!(reader.record().unwrap().is_none());
    }

    #[test]
    fn test_csv_stream_with_headers() {
        let csv = "column_a,column_b,column_c\n1,2,3\n";
        let mut reader = CsvStream::new(',', true, CopyFormat::Csv, "\\N");
        reader.write(csv.as_bytes());
        assert_eq!(reader.headers().unwrap().unwrap().get(0), Some("column_a"));
        let record = reader.record().unwrap().unwrap();
        assert_eq!(record.get(0), Some("1"));
    }

    #[test]
    fn test_csv_null_string_handling() {
        let csv = "one,\\N,three\nfour,\\N,\\N\n";
        let mut reader = CsvStream::new(',', false, CopyFormat::Csv, "\\N");
        reader.write(csv.as_bytes());

        let record = reader.record().unwrap().unwrap();
        assert_eq!(record.get(0), Some("one"));
        assert_eq!(record.get(1), Some("\\N"));
        assert_eq!(record.get(2), Some("three"));

        let record = reader.record().unwrap().unwrap();
        assert_eq!(record.get(0), Some("four"));
        assert_eq!(record.get(1), Some("\\N"));
        assert_eq!(record.get(2), Some("\\N"));

        let output = record.to_string();
        assert_eq!(output, "\"four\",\\N,\\N\n");
    }

    #[test]
    fn test_json_field_quote_escaping() {
        // Test that JSON/JSONB fields with quotes are handled properly in different formats
        // Use proper CSV escaping for the input (quotes inside CSV fields must be doubled)
        let json_data = "id,\"{\"\"name\"\":\"\"John Doe\"\",\"\"age\"\":30}\"\n";

        // Test CSV format - quotes should be escaped by doubling them
        let mut reader = CsvStream::new(',', false, CopyFormat::Csv, "\\N");
        reader.write(json_data.as_bytes());

        let record = reader.record().unwrap().unwrap();
        assert_eq!(record.get(0), Some("id"));
        assert_eq!(record.get(1), Some("{\"name\":\"John Doe\",\"age\":30}"));

        let output = record.to_string();
        assert_eq!(
            output,
            "\"id\",\"{\"\"name\"\":\"\"John Doe\"\",\"\"age\"\":30}\"\n"
        );

        // Test non-CSV format - quotes should NOT be escaped (returned as-is)
        let mut reader = CsvStream::new(',', false, CopyFormat::Text, "\\N");
        reader.write(json_data.as_bytes());

        let record = reader.record().unwrap().unwrap();
        assert_eq!(record.get(0), Some("id"));
        assert_eq!(record.get(1), Some("{\"name\":\"John Doe\",\"age\":30}"));

        let output = record.to_string();
        assert_eq!(output, "id,{\"name\":\"John Doe\",\"age\":30}\n");
    }
}
