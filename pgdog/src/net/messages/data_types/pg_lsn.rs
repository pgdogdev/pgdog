use crate::net::DataRow;

use super::Error;
use super::Format;
use super::FromDataType;
use bytes::Bytes;

#[derive(Debug, Clone, Ord, PartialEq, PartialOrd, Eq, Default, Copy)]
pub struct PgLsn {
    timeline: i64,
    position: i64,
}

impl FromDataType for PgLsn {
    fn decode(bytes: &[u8], encoding: Format) -> Result<Self, Error> {
        match encoding {
            Format::Text => {
                let s = String::decode(bytes, encoding)?;
                let mut parts = s.split("/");
                let timeline = parts.next().map(|s| i64::from_str_radix(s, 16));
                let position = parts.next().map(|s| i64::from_str_radix(s, 16));

                if let Some(Ok(timeline)) = timeline {
                    if let Some(Ok(position)) = position {
                        return Ok(Self { timeline, position });
                    }
                }

                Err(Error::UnexpectedPayload)
            }

            Format::Binary => Err(Error::NotTextEncoding),
        }
    }

    fn encode(&self, encoding: Format) -> Result<Bytes, Error> {
        match encoding {
            Format::Text => Ok(Bytes::from(format!(
                "{:X}/{:X}",
                self.timeline, self.position
            ))),
            Format::Binary => Err(Error::NotTextEncoding),
        }
    }
}

impl From<DataRow> for Option<PgLsn> {
    fn from(value: DataRow) -> Self {
        value.get(0, Format::Text)
    }
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn test_lg_lsn() {
        let lsn = "0/A9F22928";
        let lsn = PgLsn::decode(lsn.as_bytes(), Format::Text).unwrap();
        assert_eq!(lsn.timeline, 0);
        assert_eq!(lsn.position, 2_851_219_752);
    }
}
