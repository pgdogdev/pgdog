use std::{
    cmp::Ordering,
    hash::{Hash, Hasher},
    str::from_utf8,
};

use bytes::{Buf, Bytes};

use crate::{Data, DataType, Error, Format, FromDataType, ToDataRowColumn, original::Original};

/// Days from 1970-01-01 to 2000-01-01, the day Postgres counts dates from.
const POSTGRES_EPOCH_DAYS: i64 = 10_957;

/// DATE, ordered by its day number: days since 2000-01-01, as Postgres stores it.
/// `infinity` and `-infinity` are the largest and smallest day numbers.
#[derive(Debug, Clone)]
pub struct Date {
    days: i32,
    original: Original,
}

impl Date {
    /// Parse ISO text output: `2024-01-31`, `0044-03-15 BC`, `infinity`.
    ///
    /// The other `DateStyle`s print the day or the month first, and the text
    /// alone doesn't say which, so they can't be ordered.
    fn parse_text(text: &str) -> Result<i32, Error> {
        let unordered = || Error::SessionDependentText(DataType::Date);

        match text {
            "infinity" => return Ok(i32::MAX),
            "-infinity" => return Ok(i32::MIN),
            _ => {}
        }

        let (date, bc) = match text.strip_suffix(" BC") {
            Some(date) => (date, true),
            None => (text, false),
        };
        let mut fields = date.split('-');
        let (Some(year), Some(month), Some(day), None) =
            (fields.next(), fields.next(), fields.next(), fields.next())
        else {
            return Err(unordered());
        };
        // ISO pads the year to four digits; `01-31-2024` is the Postgres style.
        if year.len() < 4 || month.len() != 2 || day.len() != 2 {
            return Err(unordered());
        }
        let field = |digits: &str| {
            digits
                .bytes()
                .all(|byte| byte.is_ascii_digit())
                .then(|| digits.parse::<i64>().ok())
                .flatten()
                .ok_or_else(unordered)
        };
        let (year, month, day) = (field(year)?, field(month)?, field(day)?);
        if !(1..=12).contains(&month) || !(1..=31).contains(&day) {
            return Err(unordered());
        }

        // There is no year zero: 1 BC is the year before 1 AD.
        let year = if bc { 1 - year } else { year };

        i32::try_from(days_from_civil(year, month, day) - POSTGRES_EPOCH_DAYS)
            .map_err(|_| Error::NumericOutOfRange(DataType::Date))
    }
}

/// Days from 1970-01-01 to a date in the proleptic Gregorian calendar,
/// which is the calendar Postgres uses for every date.
fn days_from_civil(year: i64, month: i64, day: i64) -> i64 {
    // Count years from March, so a leap day is the last day of its year.
    let year = if month <= 2 { year - 1 } else { year };
    let era = year.div_euclid(400);
    let year_of_era = year.rem_euclid(400);
    let day_of_year = (153 * (month + if month > 2 { -3 } else { 9 }) + 2) / 5 + day - 1;
    let day_of_era = year_of_era * 365 + year_of_era / 4 - year_of_era / 100 + day_of_year;

    era * 146_097 + day_of_era - 719_468
}

impl PartialEq for Date {
    fn eq(&self, other: &Self) -> bool {
        self.days == other.days
    }
}

impl Eq for Date {}

impl PartialOrd for Date {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for Date {
    fn cmp(&self, other: &Self) -> Ordering {
        self.days.cmp(&other.days)
    }
}

impl Hash for Date {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.days.hash(state);
    }
}

impl FromDataType for Date {
    fn decode(bytes: &[u8], encoding: Format) -> Result<Self, Error> {
        let days = match encoding {
            Format::Text => Self::parse_text(from_utf8(bytes)?)?,
            Format::Binary => {
                if bytes.len() != 4 {
                    return Err(Error::WrongSizeBinary(bytes.len()));
                }
                let mut buf = bytes;
                buf.get_i32()
            }
        };

        Ok(Self {
            days,
            original: Original::new(bytes, encoding),
        })
    }

    fn encode(&self, encoding: Format) -> Result<Bytes, Error> {
        if let Some(bytes) = self.original.get(encoding) {
            return Ok(bytes);
        }

        match encoding {
            Format::Binary => Ok(Bytes::copy_from_slice(&self.days.to_be_bytes())),
            // Formatting depends on the server's DateStyle.
            Format::Text => Err(Error::SessionDependentText(DataType::Date)),
        }
    }
}

impl ToDataRowColumn for Date {
    fn to_data_row_column(&self) -> Data {
        self.original.bytes().clone().into()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn text(value: &str) -> Date {
        Date::decode(value.as_bytes(), Format::Text).expect(value)
    }

    fn binary(days: i32) -> Date {
        Date::decode(&days.to_be_bytes(), Format::Binary).expect("four bytes")
    }

    #[test]
    fn test_iso_text_is_the_postgres_day_number() {
        for (input, days) in [
            ("2000-01-01", 0),
            ("2000-01-02", 1),
            ("1999-12-31", -1),
            ("1970-01-01", -10_957),
            ("2024-02-29", 8_825),
            ("2024-03-01", 8_826),
            ("1900-03-01", -36_465),
            ("0001-01-01", -730_119),
            ("0001-12-31 BC", -730_120),
            // The limits of the Postgres date type.
            ("4714-11-24 BC", -2_451_545),
            ("5874897-12-31", 2_145_031_948),
            ("infinity", i32::MAX),
            ("-infinity", i32::MIN),
        ] {
            assert_eq!(text(input).days, days, "{input}");
        }
    }

    #[test]
    fn test_other_date_styles_are_not_ordered() {
        for input in [
            "01-31-2024",
            "31-01-2024",
            "01/31/2024",
            "31.01.2024",
            "2024-13-01",
            "2024-01-32",
            "2024-1-01",
            "+024-01-01",
            "",
        ] {
            assert!(
                matches!(
                    Date::decode(input.as_bytes(), Format::Text),
                    Err(Error::SessionDependentText(DataType::Date))
                ),
                "{input}"
            );
        }
    }

    #[test]
    fn test_orders_by_day_in_both_formats() {
        // The sign bit makes the bytes of every date before 2000 sort last.
        assert!(binary(-1) < binary(0));
        assert!(binary(i32::MIN) < binary(-1));
        assert!(binary(0) < binary(i32::MAX));

        // Text would put BC dates and five digit years in the wrong place.
        assert!(text("2024-01-01 BC") < text("0001-01-01"));
        assert!(text("9999-12-31") < text("10000-01-01"));
        assert!(text("-infinity") < text("4714-11-24 BC"));
        assert!(text("5874897-12-31") < text("infinity"));

        assert_eq!(binary(8_825), text("2024-02-29"));
        assert!(Date::decode(&[0; 8], Format::Binary).is_err());
    }

    #[test]
    fn test_encode_returns_original_bytes() {
        let date = text("2024-02-29");
        assert_eq!(
            date.encode(Format::Text).unwrap(),
            Bytes::from("2024-02-29")
        );
        assert_eq!(
            date.encode(Format::Binary).unwrap(),
            Bytes::copy_from_slice(&8_825_i32.to_be_bytes())
        );

        assert!(matches!(
            binary(0).encode(Format::Text),
            Err(Error::SessionDependentText(DataType::Date))
        ));
    }
}
