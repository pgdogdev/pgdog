use std::fmt;
use std::ops::Deref;

use chrono::{DateTime, Local, Offset, SubsecRound, TimeZone, Timelike, Utc};
use chrono_tz::Tz;
use pg_raw_parse::{
    ConstValue, Node, NodeMut,
    list::NodeList,
    make::{MemoryToken, Unique},
    raw::SQLValueFunctionOp,
    transform::{TransformClosure, transform_node},
};
use pgdog_stats::Relation;
use std::str::FromStr;

use crate::{
    frontend::{
        RewritePlan,
        client::QueryTimestamps,
        router::parser::{
            StatementParser, StatementRewrite, Table,
            rewrite::statement::{Error, plan::GeneratedId},
        },
    },
    net::parameter::ParameterValue,
};

/// A "parsed" time function the Client specified; either from database schema or manual commands.
/// Column type represents the data type attached, so that we can correctly assemble the String.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct TimeFunction {
    time_function_type: TimeFunctionType,
    /// TODO: What happens if the database schema is changed? This should be invalidated if cached.
    column_type: String,
}

impl TimeFunction {
    /// Based on the internal values (col type, arguments passed, ...),
    /// and the reference time (e.g., transaction time), generate
    /// the String and binary equivalent to be put in the final String.
    pub(crate) fn formatted_time(
        &self,
        timestamps: &QueryTimestamps,
        timezone_param: Option<&ParameterValue>,
    ) -> (String, Vec<u8>) {
        // TODO: Get rid of unwrap()
        let tz = timezone_param.map(|tz_str| tz_str.as_str().unwrap().parse::<Tz>().unwrap());

        let timestamp = self.column_type.eq("timestamp without time zone");

        let reference_time = match self.time_function_type.time_reference() {
            TimeReference::Current => Utc::now(),
            TimeReference::TransactionStart => timestamps.transaction_start,
            TimeReference::StatementStart => timestamps.statement_start,
        };

        let mut time_output: TimeFunctionOutput = self.time_function_type.default_output_type();

        // Column expects 'timestamp', function outputs 'timestamptz', need to convert.
        if time_output == TimeFunctionOutput::TimestampWithTimeZone && timestamp {
            time_output = TimeFunctionOutput::Timestamp
        }

        let precision = self.time_function_type.precision();

        let formatted_string = match tz {
            Some(tz) => time_output.format(&reference_time, &tz, precision),
            None => time_output.format(&reference_time, &Local, precision),
        };
        let binary = formatted_string.as_bytes().to_vec();

        (formatted_string, binary)
    }

    /// Some data types we get aren't compatible as-is with the pg_catalog type
    /// needed to specify for Bind param types / Prepare ParamRef types. This maps them to
    /// the correct pg_catalog types.
    ///
    /// Why do we need to cast? This is because, for example, if we try to use CURRENT_TIME (timetz) with a
    /// text column in a binary format Bind param, Postgres will error without an explicit cast (expected UTF-8).
    /// The alternative is manually calculating the binary format, which is a lot more of a headache! :)
    ///
    /// TODO: There might be some missing. Could be worth emitting an Error for any unexpected/untested types.
    fn col_type_to_type_cast_alias(&self) -> &str {
        match self.column_type.as_str() {
            "time with time zone" => "timetz",
            "timestamp with time zone" => "timestamptz",
            "timestamp without time zone" => "timestamp",
            "time without time zone" => "time",
            string => string,
        }
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

/// Represents the kind of `TimeFunction` that we're re-writing.
/// If an Option argument is present and Some(..), the Client specified precision.
/// <https://www.postgresql.org/docs/current/functions-datetime.html>
#[derive(Debug, Copy, Clone, PartialEq, Eq)]
pub(crate) enum TimeFunctionType {
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

/// TODO: Docs.
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
            fractional_seconds(rounded.nanosecond())
        );
        let offset = utc_offset(rounded.offset().fix().local_minus_utc());

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
}

enum TimeReference {
    /// Changes with statement execution
    Current,

    TransactionStart,

    /// "returns the start time of the current statement (more specifically,
    ///  the time of receipt of the latest command message from the client)."
    StatementStart,
}

impl TimeFunctionType {
    /// For easy iteration over all enum variants
    /// "CurrentTimestamp" is purposefully ordered before "CurrentTime" (+ LocalTimestamp/LocalTime) to prevent
    /// partial match bugs with .starts_with (we can't match on NAME() as not all require ())
    const ALL_VARIANTS: [Self; 10] = [
        Self::CurrentDate,
        Self::CurrentTimestamp(None),
        Self::CurrentTime(None),
        Self::ClockTimestamp,
        Self::LocalTimestamp(None),
        Self::LocalTime(None),
        Self::Now,
        Self::StatementTimestamp,
        Self::TimeOfDay,
        Self::TransactionTimestamp,
    ];

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

    /// Convert `TimeFunction` in VALUES list
    ///
    /// Parse both `FuncCall`s and `SQLValueFunction`s here.
    /// `now()` = `FuncCall`,
    /// `CURRENT_TIMESTAMP`, `LOCALTIME` = `SQLValueFunction`,
    fn from_node(node: Node) -> Option<Self> {
        match node {
            Node::FuncCall(func) => {
                // TODO: Look into parsing out parameters
                let Node::String(str) = func.funcname().first()? else {
                    return None;
                };
                str.sval()?.parse().ok()
            }
            Node::SQLValueFunction(func) => Self::from_sql_value_function(func.op, func.typmod),
            _ => None,
        }
    }

    /// TODO: Doc comment.
    fn from_sql_value_function(op: SQLValueFunctionOp::Type, typmod: i32) -> Option<Self> {
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

    /// Postgres formatted String to match against Client-provided names in query.
    fn name(self) -> &'static str {
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
}

/// Client `now()`.parse() -> TimeFunctionType::Now()
impl FromStr for TimeFunctionType {
    type Err = Option<Error>;

    /// TODO: Doc comment
    /// Not sure it's necessary to error in this circumstance.
    /// Would mean they didn't correctly call the function;
    /// Postgres will error them out (unless we have a logic bug)
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let s = s.to_lowercase();

        for variant in Self::ALL_VARIANTS {
            let variant_name = &variant.name();
            if s.starts_with(variant_name) {
                // TODO: I think this can be written better
                let after = s.replace(' ', "");
                let after = &after[variant_name.len()..];
                if after.starts_with('(') && after.ends_with(')') && after.len() >= 3 {
                    let after = &after[1..after.len() - 1];

                    let after_to_int: u8 = match after.parse() {
                        Ok(integer_argument) => integer_argument,
                        Err(_) => continue,
                    };

                    return Ok(match variant {
                        Self::CurrentTime(_) => Self::CurrentTime(Some(after_to_int)),
                        Self::CurrentTimestamp(_) => Self::CurrentTimestamp(Some(after_to_int)),
                        Self::LocalTime(_) => Self::LocalTime(Some(after_to_int)),
                        Self::LocalTimestamp(_) => Self::LocalTimestamp(Some(after_to_int)),
                        _ => continue,
                    });
                } else if after.eq("()") || after.is_empty() {
                    return Ok(variant);
                } else {
                    continue;
                }
            }
        }

        Err(None)
    }
}

impl StatementRewrite<'_> {
    /// Rewrites timestamp functions like now() into either ParamRefs or correctly formatted Strings,
    /// for the purpose of maintaining consistency across databases for omni tables.
    pub(super) fn rewrite_timestamp_functions<'mem, 'mutref>(
        &mut self,
        mut stmt: NodeMut<'mem, 'mutref>,
        mem: MemoryToken<'mem>,
        // TODO: Replace `next_param` with plan.param directly
        next_param: &mut i32,
        plan: &mut RewritePlan,
        timestamps: QueryTimestamps,
    ) {
        let mut parser = StatementParser::new(stmt.as_ref(), None, self.schema, None);
        let is_sharded = parser.is_sharded(self.db_schema, self.user, self.search_path);

        // not sharded = omni
        if is_sharded {
            return;
        }

        //
        let Some((relation, cols, not_covered_cols)) = self.find_not_used_cols(&mut stmt, mem)
        else {
            return;
        };

        let mut timestamp_rewrite = TimestampRewrite {
            rewrite: self,
            plan,
            next_param,
            mem,
            relation,
            cols,
            timestamps,
        };

        // 1. iterates through Schema to find DEFAULT columns
        // 2. adds the column to target list & all the values lists (ParamRef or String)
        timestamp_rewrite.handle_adding_defaults(&mut stmt, &not_covered_cols);

        // Replaces all time function calls (ParamRef or String)
        timestamp_rewrite.transform_func_calls(stmt);
    }

    /// Fetch the table Relation, so that we can get the relevant Schema for each column.
    /// Fetch the list of columns that are DEFAULT (and not already covered)
    /// TODO: cols?
    fn find_not_used_cols<'mem, 'mutref>(
        &self,
        stmt: &mut NodeMut<'mem, 'mutref>,
        mem: MemoryToken<'mem>,
    ) -> Option<(Relation, Unique<'mem, &'mem NodeList>, Vec<String>)> {
        let Node::InsertStmt(insert_stmt) = stmt.as_ref() else {
            return None;
        };

        let relation = insert_stmt.relation().expect("INSERT always has table");
        let table = Table::from(relation);

        let relation = self.db_schema.table(table, self.user, None)?;
        let cols = insert_stmt.cols();

        // Find the columns that the insert does NOT cover.
        let not_covered_cols: Vec<String> = {
            let subset: Vec<&str> = cols
                .iter()
                .filter_map(|col| match col {
                    // TODO: Replace unwrap() with an Error / None match.
                    Node::ResTarget(target) => Some(target.name().unwrap()),
                    _ => None,
                })
                .collect();

            relation
                .column_names()
                .filter(|name| !subset.contains(name))
                .map(|name| name.to_string())
                .collect()
        };

        Some((relation.clone(), mem.make_unique(cols), not_covered_cols))
    }
}

/// TODO: Doc comment
struct TimestampRewrite<'mem, 'a, 's> {
    rewrite: &'a mut StatementRewrite<'s>,
    plan: &'a mut RewritePlan,
    /// TODO: Replace `next_param` with plan.param directly
    ///       could do like a .next_param() method on `RewritePlan`
    next_param: &'a mut i32,
    mem: MemoryToken<'mem>,
    relation: Relation,
    cols: Unique<'mem, &'mem NodeList>,
    timestamps: QueryTimestamps,
}

impl<'mem, 'a, 's> TimestampRewrite<'mem, 'a, 's> {
    /// Replaces all time function calls (ParamRef or String)
    /// Used by all to handle re-writes.
    fn transform_func_calls(&mut self, stmt: NodeMut<'mem, '_>) {
        transform_node(
            stmt,
            &mut TransformClosure::new(|node| match &*node {
                // TODO: Is this guaranteed to be a VALUES list?
                //       What if it's something unrelated in the statement?
                NodeMut::NodeList(list_of_values) => {
                    // VALUES (...), (...) where (...) is what we're inspecting (one NodeList)

                    let mut cloned_values = self.mem.make_unique(list_of_values.deref());
                    let mut changed = false;

                    // The reason this is iterating over the NodeList instead of individual
                    // FuncCalls is that we must know where we are within a VALUES, as that
                    // allows us to know the present column's data type (for potential later coersion)
                    for (i, value) in list_of_values.iter().enumerate() {
                        if let Some(time_function_type) = TimeFunctionType::from_node(value) {
                            self.rewrite.rewritten = true;

                            // TODO: replace unwrap()
                            let Node::ResTarget(target) = self.cols.get(i).unwrap() else {
                                unreachable!("not cool");
                            };

                            // Get the column name, and with that, its datatype.
                            // TODO: replace unwrap()
                            let col_name = target.name().unwrap();
                            let col_relation = self.relation.columns.get(col_name).unwrap();

                            let time_function = TimeFunction {
                                time_function_type,
                                column_type: col_relation.data_type.clone(),
                            };

                            // Replace the specific node within the list.
                            cloned_values
                                .as_mut()
                                .set(i, self.make_node(&time_function));
                            changed = true;
                        }
                    }

                    // Replaces the entire VALUES list at once with the one we cloned and re-wrote.
                    if changed {
                        node.replace(cloned_values.uncast());

                        // Do not continue to traverse.
                        return None;
                    }

                    Some(node)
                }
                _ => Some(node),
            }),
        );
    }

    /// Iterates through Schema to find DEFAULT columns
    /// Adds the column to target list & all the values lists (ParamRef or String)
    fn handle_adding_defaults(
        &mut self,
        mut stmt: &mut NodeMut<'mem, '_>,
        not_covered_cols: &Vec<String>,
    ) {
        let NodeMut::InsertStmt(insert_stmt) = &mut stmt else {
            return;
        };

        for col in not_covered_cols {
            insert_stmt.cols_mut().push(
                self.mem,
                self.mem
                    .make_res_target(Some(col), self.mem.empty(), self.mem.none())
                    .uncast(),
            );

            let NodeMut::SelectStmt(select_stmt) = &mut insert_stmt.select_stmt_mut() else {
                return;
            };

            let col_relation = self.relation.columns.get(col.as_str()).unwrap();
            let Ok(time_function_type) = col_relation.column_default.parse::<TimeFunctionType>()
            else {
                continue;
            };

            let time_function = TimeFunction {
                time_function_type,
                column_type: col_relation.data_type.clone(),
            };

            // Have to add the now() to every single select now.
            // VALUES (...), (....)
            for values_list in select_stmt.values_lists_mut() {
                let mut node_list_mut = values_list.expect_node_list();

                self.rewrite.rewritten = true;
                node_list_mut.push(self.mem, self.make_node(&time_function));
            }
        }
    }

    /// If simple protocol, make an A_Const node with the String constant of the formatted time.
    /// If extended or prepare, make a ParamRef, so that we can cache it and put in the formatted time later.
    fn make_node(&mut self, time_function: &TimeFunction) -> Unique<'mem, Node<'mem>> {
        if !self.rewrite.extended || !self.rewrite.prepared {
            let source = time_function.formatted_time(&self.timestamps, self.rewrite.timezone);
            self.mem
                .make_a_const(ConstValue::String(source.0.as_str()))
                .uncast()
        } else {
            let param_ref = self.mem.make_param_ref(*self.next_param);
            *self.next_param += 1;

            // TODO: add a method to plan() for this...
            self.plan.generated_ids.push((
                (*self.next_param - 1) as u16,
                GeneratedId::ProxyTime(time_function.clone()),
            ));

            // Example: CAST($1::pg_catalog.text AS timetz)
            // This is 30x less code at the expense of query verbosity;
            // I talk about why in doc comment on `col_type_to_type_cast_alias`
            self.mem
                .make_type_cast(
                    self.mem
                        .make_type_cast(
                            param_ref.uncast(),
                            self.mem.make_list(&[
                                self.mem.make_string(Some("pg_catalog")),
                                self.mem.make_string(Some("text")),
                            ]),
                        )
                        .uncast(),
                    self.mem.make_list(&[self
                        .mem
                        .make_string(Some(time_function.col_type_to_type_cast_alias()))]),
                )
                .uncast()
        }
    }
}
