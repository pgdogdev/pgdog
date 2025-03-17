use std::fmt::Display;

use super::*;

use super::interval::bigint;

#[derive(Debug, Copy, Clone, PartialEq, Ord, PartialOrd, Eq, Default, Hash)]
pub struct Timestamp {
    pub year: i64,
    pub month: i8,
    pub day: i8,
    pub hour: i8,
    pub minute: i8,
    pub second: i8,
    pub micros: i32,
    pub offset: Option<i8>,
}

impl ToDataRowColumn for Timestamp {
    fn to_data_row_column(&self) -> Data {
        self.encode(Format::Text).unwrap().into()
    }
}

impl Display for Timestamp {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{}-{}-{} {}:{}:{}.{}",
            self.year, self.month, self.day, self.hour, self.minute, self.second, self.micros
        )?;

        if let Some(offset) = self.offset {
            write!(f, "{}{}", if offset > 0 { "+" } else { "-" }, offset)?;
        }

        Ok(())
    }
}

macro_rules! assign {
    ($result:expr, $value:tt, $parts:expr) => {
        if let Some(val) = $parts.next() {
            $result.$value = bigint(&val)?.try_into().unwrap();
        }
    };
}

impl FromDataType for Timestamp {
    fn decode(bytes: &[u8], encoding: Format) -> Result<Self, Error> {
        match encoding {
            Format::Text => {
                let s = String::decode(bytes, Format::Text)?;
                let mut result = Timestamp::default();
                let mut date_time = s.split(" ");
                let date = date_time.next();
                let time = date_time.next();

                if let Some(date) = date {
                    let mut parts = date.split("-");
                    assign!(result, year, parts);
                    assign!(result, month, parts);
                    assign!(result, day, parts);
                }

                if let Some(time) = time {
                    let mut parts = time.split(":");
                    assign!(result, hour, parts);
                    assign!(result, minute, parts);

                    if let Some(seconds) = parts.next() {
                        let mut parts = seconds.split(".");
                        assign!(result, second, parts);
                        let micros = parts.next();
                        if let Some(micros) = micros {
                            let neg = micros.find('-').is_some();
                            let mut parts = micros.split(&['-', '+']);
                            assign!(result, micros, parts);
                            if let Some(offset) = parts.next() {
                                let offset: i8 = bigint(offset)?.try_into().unwrap();
                                let offset = if neg { -offset } else { offset };
                                result.offset = Some(offset);
                            }
                        }
                        assign!(result, micros, parts);
                    }
                }

                Ok(result)
            }
            Format::Binary => Err(Error::NotTextEncoding),
        }
    }

    fn encode(&self, encoding: Format) -> Result<Bytes, Error> {
        match encoding {
            Format::Text => Ok(Bytes::copy_from_slice(self.to_string().as_bytes())),
            Format::Binary => Err(Error::NotTextEncoding),
        }
    }
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn test_timestamp() {
        let ts = "2025-03-05 14:51:42.798425".as_bytes();
        let ts = Timestamp::decode(ts, Format::Text).unwrap();

        assert_eq!(ts.year, 2025);
        assert_eq!(ts.month, 3);
        assert_eq!(ts.day, 5);
        assert_eq!(ts.hour, 14);
        assert_eq!(ts.minute, 51);
        assert_eq!(ts.second, 42);
        assert_eq!(ts.micros, 798425);
    }
}
