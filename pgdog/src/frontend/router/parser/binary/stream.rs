use super::{super::Error, header::Header, tuple::Tuple};

#[derive(Clone, Default)]
pub struct BinaryStream {
    header: Option<Header>,
    buffer: Vec<u8>,
}

impl std::fmt::Debug for BinaryStream {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("BinaryStream")
            .field("header", &self.header)
            .field("buffer", &self.buffer.len())
            .finish()
    }
}

impl BinaryStream {
    pub fn write(&mut self, bytes: &[u8]) {
        self.buffer.extend(bytes);
    }

    pub fn tuple(&mut self) -> Result<Option<Tuple>, Error> {
        loop {
            if let Some(header) = &self.header {
                if let Some(tuple) = Tuple::read(header, &mut self.buffer.as_slice()) {
                    self.buffer = Vec::from(&self.buffer[tuple.bytes_read(header)..]);
                    return Ok(Some(tuple));
                } else {
                    return Ok(None);
                }
            } else {
                match self.header()? {
                    Some(_) => continue,
                    None => return Ok(None),
                }
            }
        }
    }

    pub fn tuples(&mut self) -> Iter<'_> {
        Iter::new(self)
    }

    pub fn header(&mut self) -> Result<Option<&Header>, Error> {
        if self.header.is_some() {
            Ok(self.header.as_ref())
        } else {
            let header = Header::read(&mut self.buffer.as_slice())?;
            if let Some(header) = header {
                self.buffer = Vec::from(&self.buffer[header.bytes_read()..]);
                self.header = Some(header);
                Ok(self.header.as_ref())
            } else {
                Ok(None)
            }
        }
    }
}

pub struct Iter<'a> {
    stream: &'a mut BinaryStream,
}

impl<'a> Iter<'a> {
    pub(super) fn new(stream: &'a mut BinaryStream) -> Self {
        Self { stream }
    }
}

impl Iterator for Iter<'_> {
    type Item = Result<Tuple, Error>;

    fn next(&mut self) -> Option<Self::Item> {
        self.stream.tuple().transpose()
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::frontend::router::parser::binary::Data;

    fn make_binary_header() -> Vec<u8> {
        let mut data = b"PGCOPY\n".to_vec();
        data.push(255);
        data.push(b'\r');
        data.push(b'\n');
        data.push(b'\0');
        data.extend(0_i32.to_be_bytes()); // flags
        data.extend(0_i32.to_be_bytes()); // extension
        data
    }

    fn make_tuple(columns: &[&[u8]]) -> Vec<u8> {
        let mut data = Vec::new();
        data.extend((columns.len() as i16).to_be_bytes());
        for col in columns {
            data.extend((col.len() as i32).to_be_bytes());
            data.extend(*col);
        }
        data
    }

    fn make_terminator() -> Vec<u8> {
        (-1_i16).to_be_bytes().to_vec()
    }

    fn assert_column_eq(data: &Data, expected: &[u8]) {
        match data {
            Data::Column(bytes) => assert_eq!(&bytes[..], expected),
            Data::Null => panic!("Expected column data, got Null"),
        }
    }

    #[test]
    fn test_binary_stream_chunked_header() {
        // Test that binary stream handles header split across multiple chunks
        let header = make_binary_header();
        let tuple = make_tuple(&[b"hello", b"world"]);
        let terminator = make_terminator();

        let mut stream = BinaryStream::default();

        // Write only first 5 bytes of header (header is 19 bytes total)
        stream.write(&header[..5]);

        // Should return None, not error
        let result = stream.tuple();
        assert!(result.is_ok(), "Should not error on partial header");
        assert!(
            result.unwrap().is_none(),
            "Should return None for partial header"
        );

        // Write rest of header
        stream.write(&header[5..]);

        // Write tuple
        stream.write(&tuple);

        // Now should get tuple with correct data
        let t = stream.tuple().unwrap().unwrap();
        assert!(!t.end());
        assert_eq!(t.len(), 2);
        assert_column_eq(&t[0], b"hello");
        assert_column_eq(&t[1], b"world");

        // Write terminator
        stream.write(&terminator);
        let term = stream.tuple().unwrap().unwrap();
        assert!(term.end());
    }

    #[test]
    fn test_binary_stream_chunked_tuple() {
        // Test that binary stream handles tuple split across multiple chunks
        let header = make_binary_header();
        let tuple = make_tuple(&[b"hello", b"world"]);
        let terminator = make_terminator();

        let mut stream = BinaryStream::default();

        // Write complete header
        stream.write(&header);

        // Write only first 3 bytes of tuple (num_cols + partial length)
        stream.write(&tuple[..3]);

        // Should return None, not error
        let result = stream.tuple();
        assert!(result.is_ok(), "Should not error on partial tuple");
        assert!(
            result.unwrap().is_none(),
            "Should return None for partial tuple"
        );

        // Write rest of tuple
        stream.write(&tuple[3..]);

        // Now should get tuple with correct data
        let t = stream.tuple().unwrap().unwrap();
        assert!(!t.end());
        assert_eq!(t.len(), 2);
        assert_column_eq(&t[0], b"hello");
        assert_column_eq(&t[1], b"world");

        // Write terminator
        stream.write(&terminator);
        let term = stream.tuple().unwrap().unwrap();
        assert!(term.end());
    }

    #[test]
    fn test_binary_stream_complete_data() {
        // Test that complete data in one chunk still works
        let header = make_binary_header();
        let tuple1 = make_tuple(&[b"hello", b"world"]);
        let tuple2 = make_tuple(&[b"foo", b"bar"]);
        let terminator = make_terminator();

        let mut stream = BinaryStream::default();

        // Write all data at once
        stream.write(&header);
        stream.write(&tuple1);
        stream.write(&tuple2);
        stream.write(&terminator);

        // Should get first tuple with correct data
        let t1 = stream.tuple().unwrap().unwrap();
        assert!(!t1.end());
        assert_eq!(t1.len(), 2);
        assert_column_eq(&t1[0], b"hello");
        assert_column_eq(&t1[1], b"world");

        // Should get second tuple with correct data
        let t2 = stream.tuple().unwrap().unwrap();
        assert!(!t2.end());
        assert_eq!(t2.len(), 2);
        assert_column_eq(&t2[0], b"foo");
        assert_column_eq(&t2[1], b"bar");

        // Should get terminator
        let term = stream.tuple().unwrap().unwrap();
        assert!(term.end());

        // No more data
        assert!(stream.tuple().unwrap().is_none());
    }
}
