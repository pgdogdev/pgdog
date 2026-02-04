use csv_core::{ReadRecordResult, Reader, ReaderBuilder};

pub mod iterator;
pub mod record;

pub use iterator::Iter;
pub use record::Record;

use super::CopyFormat;

static RECORD_BUFFER: usize = 4096;
static ENDS_BUFFER: usize = 2048; // Max of 2048 columns in a CSV.
                                  // Postgres supports a max of 1600 columns in a table,
                                  // so we are well within bounds.

/// CSV reader that can handle partial inputs.
#[derive(Clone)]
pub struct CsvStream {
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
    pub fn new(delimiter: char, headers: bool, format: CopyFormat, null_string: &str) -> Self {
        Self {
            buffer: Vec::new(),
            record: vec![0u8; RECORD_BUFFER],
            ends: vec![0usize; ENDS_BUFFER],
            reader: Self::reader(delimiter, format),
            read: 0,
            null_string: null_string.to_owned(),
            delimiter,
            headers,
            headers_record: None,
            format,
        }
    }

    fn reader(delimiter: char, format: CopyFormat) -> Reader {
        let mut builder = ReaderBuilder::new();
        builder.delimiter(delimiter as u8);

        // PostgreSQL TEXT format doesn't use CSV-style quoting.
        // Only enable quoting for CSV format.
        if format == CopyFormat::Csv {
            builder.double_quote(true);
        } else {
            builder.quoting(false);
        }

        builder.build()
    }

    /// Write some data to the CSV stream.
    ///
    /// This data will be appended to the input buffer. To read records from
    /// that stream, call [`Self::record`].
    pub fn write(&mut self, data: &[u8]) {
        self.buffer.extend(data);
    }

    /// Fetch a record from the stream. This mutates the inner buffer,
    /// so you can only fetch the record once.
    pub fn record(&mut self) -> Result<Option<Record>, super::Error> {
        loop {
            let (result, read, written, ends) = self.reader.read_record(
                &self.buffer[self.read..],
                &mut self.record,
                &mut self.ends,
            );

            match result {
                ReadRecordResult::OutputFull => {
                    self.record.resize(self.buffer.len() * 2 + 1, 0u8);
                    self.reader = Self::reader(self.delimiter, self.format);
                }

                // Data incomplete.
                ReadRecordResult::InputEmpty | ReadRecordResult::End => {
                    self.buffer = Vec::from(&self.buffer[self.read..]);
                    self.read = 0;
                    self.reader = Self::reader(self.delimiter, self.format);
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
    pub fn records(&mut self) -> Iter<'_> {
        Iter::new(self)
    }

    /// Get headers from the CSV, if any.
    pub fn headers(&mut self) -> Result<Option<&Record>, super::Error> {
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
        // Test that JSON/JSONB fields with quotes are handled properly in CSV format
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
    }

    #[test]
    fn test_text_format_json_no_quoting() {
        // Test TEXT format with JSON data - TEXT format doesn't use CSV-style quoting
        // so quotes are treated as literal characters and NOT stripped.
        // This uses tab delimiter (PostgreSQL TEXT format default)
        let json_data = "id\t{\"name\":\"John Doe\",\"age\":30}\n";

        let mut reader = CsvStream::new('\t', false, CopyFormat::Text, "\\N");
        reader.write(json_data.as_bytes());

        let record = reader.record().unwrap().unwrap();
        assert_eq!(record.get(0), Some("id"));
        // Quotes are preserved as-is (not stripped like CSV format would)
        assert_eq!(record.get(1), Some("{\"name\":\"John Doe\",\"age\":30}"));

        let output = record.to_string();
        // TEXT format outputs data as-is, no quote escaping
        assert_eq!(output, "id\t{\"name\":\"John Doe\",\"age\":30}\n");
    }

    #[test]
    fn test_text_format_composite_type() {
        // PostgreSQL composite type with quoted field inside: (,Annapolis,Maryland,"United States",)
        // In TEXT format with tab delimiter, this should be preserved exactly as-is.
        let data = "1\t(,Annapolis,Maryland,\"United States\",)\n";
        let mut reader = CsvStream::new('\t', false, CopyFormat::Text, "\\N");
        reader.write(data.as_bytes());

        let record = reader.record().unwrap().unwrap();
        assert_eq!(record.get(0), Some("1"));
        // The composite type field should preserve the quotes
        assert_eq!(
            record.get(1),
            Some("(,Annapolis,Maryland,\"United States\",)"),
            "Composite type quotes should be preserved"
        );

        let output = record.to_string();
        assert_eq!(
            output, data,
            "Text format output should match input exactly"
        );
    }

    #[test]
    fn test_text_format_composite_type_with_quotes() {
        // Test that composite types with internal quotes are handled correctly.
        // PostgreSQL composite type: (,Annapolis,Maryland,"United States",)
        // In TEXT format with tab delimiter, quotes should be preserved exactly.
        let data = b"1\t(,Annapolis,Maryland,\"United States\",)\n";
        let mut reader = CsvStream::new('\t', false, CopyFormat::Text, "\\N");
        reader.write(data);

        let record = reader.record().unwrap().unwrap();

        assert_eq!(
            record.len(),
            2,
            "Should have exactly 2 tab-separated fields"
        );
        assert_eq!(record.get(0), Some("1"));
        assert_eq!(
            record.get(1),
            Some("(,Annapolis,Maryland,\"United States\",)"),
            "Composite type with quoted field should preserve quotes"
        );

        let output = record.to_string();
        assert_eq!(
            output.as_bytes(),
            data,
            "Text format output should match input exactly"
        );
    }

    #[test]
    fn test_text_format_chunked_data() {
        // Test that TEXT format correctly handles chunked data with quotes
        let mut reader = CsvStream::new('\t', false, CopyFormat::Text, "\\N");

        // First chunk ends right before the quote
        let chunk1 = b"1\t(,Annapolis,Maryland,";
        reader.write(chunk1);
        let record = reader.record().unwrap();
        assert!(record.is_none(), "Should not have complete record yet");

        // Second chunk contains the quoted portion
        let chunk2 = b"\"United States\",)\n";
        reader.write(chunk2);
        let record = reader.record().unwrap().expect("Should have record now");

        assert_eq!(record.get(0), Some("1"));
        assert_eq!(
            record.get(1),
            Some("(,Annapolis,Maryland,\"United States\",)"),
            "Chunked data should preserve quotes in TEXT format"
        );
    }

    #[test]
    fn test_text_format_preserves_quotes() {
        // Verify TEXT format doesn't strip quotes (unlike CSV format)
        // This is critical for composite types with quoted fields
        use csv_core::ReaderBuilder;

        let input = b"\"United States\"\n";

        // CSV-style quoting strips quotes
        let mut reader_csv = ReaderBuilder::new()
            .delimiter(b'\t')
            .double_quote(true)
            .build();
        let mut output = [0u8; 1024];
        let mut ends = [0usize; 16];
        let (_, _, written, _) = reader_csv.read_record(input, &mut output, &mut ends);
        let csv_result = std::str::from_utf8(&output[..written]).unwrap();
        assert_eq!(csv_result, "United States", "CSV quoting strips quotes");

        // TEXT format (quoting disabled) preserves quotes
        let mut reader_text = ReaderBuilder::new().delimiter(b'\t').quoting(false).build();
        let (_, _, written, _) = reader_text.read_record(input, &mut output, &mut ends);
        let text_result = std::str::from_utf8(&output[..written]).unwrap();
        assert_eq!(
            text_result, "\"United States\"",
            "TEXT format preserves quotes"
        );
    }
}
