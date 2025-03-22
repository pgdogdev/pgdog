use super::{super::Error, header::Header, tuple::Tuple};

#[derive(Debug, Clone)]
pub struct BinaryStream {
    header: Option<Header>,
    buffer: Vec<u8>,
}

impl BinaryStream {
    pub fn write(&mut self, bytes: &[u8]) {
        self.buffer.extend(bytes);
    }

    pub fn tuple(&mut self) -> Result<Option<Tuple>, Error> {
        loop {
            if let Some(header) = &self.header {
                let tuple = Tuple::read(header, &mut self.buffer.as_slice())?;
                if let Some(tuple) = tuple {
                    self.buffer = Vec::from(&self.buffer[..tuple.bytes_read(header)]);
                    return Ok(Some(tuple));
                } else {
                    return Ok(None);
                }
            } else {
                let header = Header::read(&mut self.buffer.as_slice())?;
                self.buffer = Vec::from(&self.buffer[..header.bytes_read()]);
                self.header = Some(header);
            }
        }
    }

    pub fn tuples(&mut self) -> Iter<'_> {
        Iter::new(self)
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
