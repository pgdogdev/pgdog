use std::fmt;

use chrono::{DateTime, Offset, SubsecRound, TimeZone, Timelike, Utc};
use chrono_tz::Tz;

use crate::{
    frontend::{
        client::QueryTimestamps,
        router::parser::rewrite::statement::{Error, non_deterministic_funcs::NDFunctionType},
    },
    net::parameter::ParameterValue,
};

use pg_raw_parse::raw::SQLValueFunctionOp;

/// Represents what Postgres type the `TimeFunction` would normally output.
#[derive(PartialEq)]
enum TimeFunctionOutput {
    Date,
    TimeWithTimeZone,
    TimestampWithTimeZone,
    Time,
    Timestamp,

    /// Specially formatted (e.g. EST instead of -05) as it's intended for a text col.
    TextFormattedTimestampWithTimeZone,
}

impl TimeFunctionOutput {
    /// Formats `utc_time` how Postgres outputs for this time func. Timezone taken into account (`tz`).
    /// Seconds with fractions rounded to `precision` (which are capped by Postgres at 6)
    fn format<Z>(&self, utc_time: &DateTime<Utc>, tz: &Z, precision: u8) -> String
    where
        Z: TimeZone,
        Z::Offset: fmt::Display,
    {
        let local_time = utc_time.with_timezone(tz);
        let rounded = local_time
            .clone()
            .round_subsecs(u16::from(precision.min(6)));

        let date = rounded.format("%Y-%m-%d");
        let time = format!(
            "{}{}",
            rounded.format("%H:%M:%S"),
            Self::fractional_seconds(rounded.nanosecond())
        );
        let offset = Self::utc_offset(rounded.offset().fix().local_minus_utc());

        match self {
            Self::Date => local_time.format("%Y-%m-%d").to_string(),
            Self::Time => time,
            Self::TimeWithTimeZone => format!("{time}{offset}"),
            Self::Timestamp => format!("{date} {time}"),
            Self::TimestampWithTimeZone => format!("{date} {time}{offset}"),
            Self::TextFormattedTimestampWithTimeZone => format!(
                "{}.{:06} {}",
                local_time.format("%a %b %d %H:%M:%S"),
                local_time.nanosecond() / 1_000,
                local_time.format("%Y %Z"),
            ),
        }
    }

    /// Postgres trims trailing zeros from fractional seconds
    /// It also drops the dot when there's none
    fn fractional_seconds(nanoseconds: u32) -> String {
        let microseconds = nanoseconds / 1_000;

        if microseconds == 0 {
            return String::new();
        }

        format!(".{microseconds:06}")
            .trim_end_matches('0')
            .to_string()
    }

    /// Postgres prints UTC offsets as +HH
    /// Adds :MM and :SS when they're non-zero.
    fn utc_offset(local_minus_utc: i32) -> String {
        let sign = if local_minus_utc < 0 { '-' } else { '+' };
        let total_seconds = local_minus_utc.unsigned_abs();
        let (hours, minutes, seconds) = (
            total_seconds / 3600,
            total_seconds / 60 % 60,
            total_seconds % 60,
        );

        match (minutes, seconds) {
            (0, 0) => format!("{sign}{hours:02}"),
            (_, 0) => format!("{sign}{hours:02}:{minutes:02}"),
            _ => format!("{sign}{hours:02}:{minutes:02}:{seconds:02}"),
        }
    }
}

#[derive(PartialEq)]
enum TimeReference {
    /// Changes with statement execution
    Current,

    /// Time when a transaction is started; or, when an implicit transaction,
    /// is the same as `StatementStart`.
    TransactionStart,

    /// "returns the start time of the current statement (more specifically,
    ///  the time of receipt of the latest command message from the client)."
    StatementStart,
}

/// Represents the kind of `TimeFunction` that we're re-writing.
/// If an Option argument is present and Some(..), the Client specified precision.
/// <https://www.postgresql.org/docs/current/functions-datetime.html>
#[derive(Debug, Copy, Clone, PartialEq, Eq)]
pub(super) enum TimeFunctionType {
    CurrentDate,
    CurrentTime(Option<u8>),
    CurrentTimestamp(Option<u8>),
    ClockTimestamp,
    LocalTime(Option<u8>),
    LocalTimestamp(Option<u8>),
    Now,
    StatementTimestamp,
    TimeOfDay,
    TransactionTimestamp,
}

impl TimeFunctionType {
    /// For easy iteration over all enum variants
    /// "CurrentTimestamp" is purposefully ordered before "CurrentTime" (+ LocalTimestamp/LocalTime) to prevent
    /// partial match bugs with .starts_with (we can't match on NAME() as not all require ())
    pub(super) const ALL_VARIANTS: [NDFunctionType; 10] = [
        NDFunctionType::TimeFunction(Self::CurrentDate),
        NDFunctionType::TimeFunction(Self::CurrentTimestamp(None)),
        NDFunctionType::TimeFunction(Self::CurrentTime(None)),
        NDFunctionType::TimeFunction(Self::ClockTimestamp),
        NDFunctionType::TimeFunction(Self::LocalTimestamp(None)),
        NDFunctionType::TimeFunction(Self::LocalTime(None)),
        NDFunctionType::TimeFunction(Self::Now),
        NDFunctionType::TimeFunction(Self::StatementTimestamp),
        NDFunctionType::TimeFunction(Self::TimeOfDay),
        NDFunctionType::TimeFunction(Self::TransactionTimestamp),
    ];

    /// If the type has a parameter (for precision), return the same type with that parameter.
    pub(super) fn with_param(self, precision: u8) -> Self {
        match self {
            Self::CurrentTime(_) => Self::CurrentTime(Some(precision)),
            Self::CurrentTimestamp(_) => Self::CurrentTimestamp(Some(precision)),
            Self::LocalTime(_) => Self::LocalTime(Some(precision)),
            Self::LocalTimestamp(_) => Self::LocalTimestamp(Some(precision)),
            _ => self,
        }
    }

    /// Based on the internal values (col type, arguments passed, ...),
    /// and the reference time (e.g., transaction time), generate
    /// the String and binary equivalent to be put in the final String.
    pub(super) fn formatted_time(
        &self,
        column_type: &str,
        timestamps: &QueryTimestamps,
        timezone_param: Option<&ParameterValue>,
    ) -> Result<String, Error> {
        let timestamp = column_type.eq("timestamp without time zone");

        let reference_time = match self.time_reference() {
            TimeReference::Current => Utc::now(),
            TimeReference::TransactionStart => timestamps.transaction_start,
            TimeReference::StatementStart => timestamps.statement_start,
        };

        let mut time_output: TimeFunctionOutput = self.default_output_type();

        // Column expects 'timestamp', function outputs 'timestamptz', need to convert.
        if time_output == TimeFunctionOutput::TimestampWithTimeZone && timestamp {
            time_output = TimeFunctionOutput::Timestamp
        }

        let precision = self.precision();

        let tz = match Self::session_time_zone(timezone_param) {
            Ok(tz) => tz,
            Err(_) if time_output == TimeFunctionOutput::TimestampWithTimeZone => Tz::UTC,
            Err(err) => return Err(err),
        };

        Ok(time_output.format(&reference_time, &tz, precision))
    }

    /// The precision of partial seconds that should be displayed in the `TimeFunction`'s output.
    fn precision(self) -> u8 {
        match self {
            Self::CurrentTime(precision)
            | Self::LocalTime(precision)
            | Self::LocalTimestamp(precision)
            | Self::CurrentTimestamp(precision) => precision,
            _ => None,
        }
        .unwrap_or(6)
    }

    /// What point of time (current, transaction start, statement start) should we base the
    /// `TimeFunction`'s output on?
    fn time_reference(self) -> TimeReference {
        match self {
            Self::ClockTimestamp | Self::TimeOfDay => TimeReference::Current,
            Self::CurrentDate
            | Self::CurrentTime(_)
            | Self::CurrentTimestamp(_)
            | Self::LocalTime(_)
            | Self::LocalTimestamp(_)
            | Self::TransactionTimestamp
            | Self::Now => TimeReference::TransactionStart,
            Self::StatementTimestamp => TimeReference::StatementStart,
        }
    }

    /// If we're considering re-writing a `TimeFunction`, the decision as to whether or not we should
    /// rewrite rests solely on the corresponding `TimeReference` being `TransactionStart`. Otherwise,
    /// there's no point; Postgres can achieve the same functionality without our assistance.
    pub(super) fn apply_rewrite_on_sharded_tables(&self) -> bool {
        self.time_reference() == TimeReference::TransactionStart
    }

    /// Represents what Postgres type the `TimeFunction` would normally output.
    fn default_output_type(self) -> TimeFunctionOutput {
        match self {
            Self::CurrentTimestamp(_)
            | Self::ClockTimestamp
            | Self::Now
            | Self::StatementTimestamp
            | Self::TransactionTimestamp => TimeFunctionOutput::TimestampWithTimeZone,
            Self::CurrentTime(_) => TimeFunctionOutput::TimeWithTimeZone,
            Self::CurrentDate => TimeFunctionOutput::Date,
            Self::LocalTime(_) => TimeFunctionOutput::Time,
            Self::LocalTimestamp(_) => TimeFunctionOutput::Timestamp,
            Self::TimeOfDay => TimeFunctionOutput::TextFormattedTimestampWithTimeZone,
        }
    }

    /// The session's `TimeZone` as an IANA name, e.g. `UTC` or `America/New_York`.
    ///
    /// TODO: Offsets like `+00:00` or `<-08>+08` are future work.
    fn session_time_zone(timezone: Option<&ParameterValue>) -> Result<Tz, Error> {
        let timezone = timezone.ok_or(Error::UnknownTimeZone)?;
        timezone
            .as_str()
            .and_then(|value| value.parse::<Tz>().ok())
            .ok_or_else(|| Error::UnsupportedTimeZone(timezone.to_string()))
    }

    /// Postgres formatted String to match against Client-provided names in query.
    pub(super) fn name(self) -> &'static str {
        match self {
            Self::CurrentDate => "current_date",
            Self::CurrentTime(_) => "current_time",
            Self::CurrentTimestamp(_) => "current_timestamp",
            Self::ClockTimestamp => "clock_timestamp",
            Self::LocalTime(_) => "localtime",
            Self::LocalTimestamp(_) => "localtimestamp",
            Self::Now => "now",
            Self::StatementTimestamp => "statement_timestamp",
            Self::TimeOfDay => "timeofday",
            Self::TransactionTimestamp => "transaction_timestamp",
        }
    }

    /// Convert `SQLValueFunctionOp` (e.g. current_date, current_time... non ()) to `TimeFunctionType`
    pub(super) fn from_sql_value_function(
        op: SQLValueFunctionOp::Type,
        typmod: i32,
    ) -> Option<Self> {
        use SQLValueFunctionOp::*;

        let precision = u8::try_from(typmod).ok();

        Some(match op {
            SVFOP_CURRENT_DATE => Self::CurrentDate,
            SVFOP_CURRENT_TIME => Self::CurrentTime(None),
            SVFOP_CURRENT_TIME_N => Self::CurrentTime(precision),
            SVFOP_CURRENT_TIMESTAMP => Self::CurrentTimestamp(None),
            SVFOP_CURRENT_TIMESTAMP_N => Self::CurrentTimestamp(precision),
            SVFOP_LOCALTIME => Self::LocalTime(None),
            SVFOP_LOCALTIME_N => Self::LocalTime(precision),
            SVFOP_LOCALTIMESTAMP => Self::LocalTimestamp(None),
            SVFOP_LOCALTIMESTAMP_N => Self::LocalTimestamp(precision),
            // Others: CURRENT_USER, CURRENT_SCHEMA... not relevant here
            _ => return None,
        })
    }
}
