use pg_raw_parse::{
    ConstValue, Node, NodeMut,
    list::NodeList,
    make::{MemoryToken, Unique},
    raw::SQLValueFunctionOp,
    transform::Transform,
};
use pgdog_stats::{Column, Relation};

use super::BindParams;
use crate::{
    frontend::{
        client::QueryTimestamps,
        router::parser::{
            StatementParser, StatementRewrite, Table,
            rewrite::statement::{
                Error,
                non_deterministic_funcs::{
                    rewrite_transaction_time::ReplaceTransactionTime,
                    time_function::TimeFunctionType, uuid_function::UUIDFunctionType,
                },
                plan::BindParam,
            },
        },
    },
    net::parameter::ParameterValue,
};

/// No need to expose these outside.
mod rewrite_insert;
mod rewrite_transaction_time;
mod time_function;
mod uuid_function;

/// A non-deterministic function that we must re-write when writing to an omnisharded table (or now() for sharded),
/// so that we can maintain consistency instead of generating a different value (from executing the function)
/// on each shard. This re-writes function calls to a constant.
#[derive(Debug, PartialEq, Eq, Clone)]
pub(crate) struct NDFunction {
    nd_function_type: NDFunctionType,

    /// TODO: What happens if the database schema is changed? This should be invalidated if cached.
    column_type: String,
}

impl NDFunction {
    /// Generate a Postgres-ready String and binary equivalent for the non-deterministic function.
    /// Binary format is always text-based as we explicitly inner-cast the ParamRefs we use with ::text
    /// (and have an outer re-cast into the actual column data type)
    pub(crate) fn write_as_constant(
        &self,
        timestamps: &QueryTimestamps,
        timezone_param: Option<&ParameterValue>,
    ) -> Result<(String, Vec<u8>), Error> {
        let formatted_string = match self.nd_function_type {
            NDFunctionType::TimeFunction(tf) => {
                tf.formatted_time(&self.column_type, timestamps, timezone_param)
            }
            NDFunctionType::UUIDFunction(uuid) => uuid.format(),
        }?;

        let binary = formatted_string.as_bytes().to_vec();
        Ok((formatted_string, binary))
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
            other => other,
        }
    }
}

/// Simple wrapper around `TimeFunctionType` and `UUIDFunctionType` to pass functions through
/// depending on the type of non-deterministic function that was parsed.
#[derive(Debug, Clone, PartialEq, Eq, Copy)]
enum NDFunctionType {
    TimeFunction(TimeFunctionType),
    UUIDFunction(UUIDFunctionType),
}

/// We cover two cases in this code: (1) transaction time functions (SELECT, all INSERT) and (2) omnisharded INSERT
#[derive(PartialEq)]
enum RewriteCase {
    TransactionTimeFunction,
    OmnishardedInsert,
}

impl NDFunctionType {
    /// Convert `SQLValueFunctionOp` (e.g. current_date, current_time... non ()) to `NDFunctionType`
    fn from_sql_value_function(
        op: SQLValueFunctionOp::Type,
        typmod: i32,
        rewrite_case: &RewriteCase,
    ) -> Option<Self> {
        let nd_func_type = TimeFunctionType::from_sql_value_function(op, typmod)
            .map(NDFunctionType::TimeFunction)
            .or_else(|| {
                UUIDFunctionType::from_sql_value_function(op, typmod)
                    .map(NDFunctionType::UUIDFunction)
            });

        // Verify the `SQLValueFunction` is transaction time based, if we're
        // only re-writing transaction time functions.
        if let Some(nd_func) = nd_func_type
            && *rewrite_case == RewriteCase::TransactionTimeFunction
            && !nd_func.is_transaction_time_function()
        {
            return None;
        }

        nd_func_type
    }

    /// Postgres-formatted function name for this function type.
    /// Used for pattern matching to determine what kind of function (if any) is present.
    fn name(&self) -> &str {
        match self {
            Self::TimeFunction(tf) => tf.name(),
            Self::UUIDFunction(uuid) => uuid.name(),
        }
    }

    /// If the type has a parameter (for precision), return the same type with that parameter.
    fn with_param(&self, param: u8) -> Self {
        match self {
            Self::TimeFunction(tf) => Self::TimeFunction(tf.with_param(param)),
            Self::UUIDFunction(uuid) => Self::UUIDFunction(uuid.with_param()),
        }
    }

    /// Should we apply a re-write to a `ShardedTable`?
    /// This is mostly relevant to maintaining consistency in a Transaction affecting multiple Shards,
    /// where our `TimeReference` is `TransactionStart`.
    ///
    /// Why not re-write everything? This implementation isn't perfect; some things aren't implemented (e.g. interval arg for uuidv7).
    /// Best not to do anything we don't have to; otherwise, it could unnecessarily break something for someone.
    /// (maybe they're reliant on the config setting for something else)
    fn is_transaction_time_function(&self) -> bool {
        match self {
            Self::TimeFunction(tf) => tf.is_transaction_time_function(),
            Self::UUIDFunction(_) => false,
        }
    }

    /// Used to determine what type the Postgres function outputs by default, so we can cast the text into it
    /// in cases like SELECT where we don't have a corresponding column type to go by.
    fn output_type_as_str(&self) -> &'static str {
        match self {
            Self::TimeFunction(tf) => tf.default_output_type().into_postgres_str(),
            Self::UUIDFunction(_) => "uuid",
        }
    }

    /// Convert `TimeFunction` in VALUES list
    ///
    /// Parse both `FuncCall`s and `SQLValueFunction`s here.
    /// `now()` = `FuncCall`,
    /// `CURRENT_TIMESTAMP`, `LOCALTIME` = `SQLValueFunction`,
    fn from_node(
        node: Node,
        column_relation: Option<&Column>,
        rewrite_case: &RewriteCase,
    ) -> Result<Option<Self>, Error> {
        match node {
            Node::FuncCall(func) => {
                let Some(Node::String(str)) = func.funcname().first() else {
                    return Ok(None);
                };

                let Some(func_name) = str.sval() else {
                    return Ok(None);
                };

                Self::from_func_call(func_name, Some(func.args()), rewrite_case)
            }
            Node::SQLValueFunction(func) => Ok(Self::from_sql_value_function(
                func.op,
                func.typmod,
                rewrite_case,
            )),

            // If DEFAULT is in a VALUES list; fetch the column based on index.
            Node::SetToDefault(_) => {
                if let Some(relation) = column_relation {
                    return Self::from_func_call(
                        relation.column_default.as_str(),
                        None,
                        rewrite_case,
                    );
                }

                Ok(None)
            }

            _ => Ok(None),
        }
    }

    /// Parse the function to determine if it's one we should re-write (date/time, uuid)
    /// If it is, return the corresponding `NDFunctionType`.
    ///
    /// from_func_call(Client `now()`) -> NDFunction::TimeFunction(TimeFunctionType::Now())
    /// This doesn't use the FromStr trait because I wanted to return an `Option` type
    ///
    /// Returns an Error when the argument is not supported by us yet (e.g. intervals for uuidv7())
    fn from_func_call(
        func_name: &str,
        args: Option<&NodeList>,
        case: &RewriteCase,
    ) -> Result<Option<Self>, Error> {
        // Normalize the function name. Postgres does this.
        let func_name = func_name.to_lowercase();

        for variant in TimeFunctionType::ALL_VARIANTS
            .iter()
            .chain(UUIDFunctionType::ALL_VARIANTS.iter())
        {
            // If we're re-writing for a transaction time function, and
            // the current variant we're looking at isn't one, then
            // we should skip.
            if *case == RewriteCase::TransactionTimeFunction
                && !variant.is_transaction_time_function()
            {
                continue;
            }

            let variant_name = &variant.name();
            if func_name.starts_with(variant_name) {
                // If `args` is None, this means that it was passed by Schema (DEFAULT), where we haven't run pg_raw_parse
                // TODO: pg_raw_parse could (potentially) parse this instead.
                if args.is_none() {
                    let func_name = func_name.replace(' ', "");
                    let arguments = &func_name[variant_name.len()..];

                    if arguments.starts_with('(')
                        && arguments.ends_with(')')
                        && arguments.len() >= 3
                    {
                        let arguments = &arguments[1..arguments.len() - 1];

                        let after_to_int: u8 = match arguments.parse() {
                            Ok(integer_argument) => integer_argument,
                            Err(_) => {
                                // Unsupported argument in Schema
                                return Err(Error::UnsupportedArgument(variant_name.to_string()));
                            }
                        };

                        return Ok(Some(variant.with_param(after_to_int)));
                    } else if arguments.eq("()") || arguments.is_empty() {
                        return Ok(Some(*variant));
                    } else {
                        continue;
                    }
                } else if let Some(args) = args {
                    if args.len() >= 2 {
                        // Only support a singular parameter right now;
                        // none of our current functions require more than that.
                        return Err(Error::UnsupportedArgument(variant_name.to_string()));
                    }

                    let Some(first_arg) = args.get(0) else {
                        // No arguments. As-is.
                        return Ok(Some(*variant));
                    };

                    if let Node::A_Const(constant) = first_arg
                        && let Some(constant) = constant.val()
                        && let Some(constant_number) = constant.numeric_value::<i32>()
                    {
                        match constant_number.try_into() {
                            Ok(constant_number) => {
                                return Ok(Some(variant.with_param(constant_number)));
                            }
                            Err(_) => {
                                return Err(Error::UnsupportedArgument(variant_name.to_string()));
                            }
                        }
                    } else {
                        // Only support parsing out a numeric constant right now.
                        // TODO: uuidv7 param
                        return Err(Error::UnsupportedArgument(variant_name.to_string()));
                    }
                }
            }
        }

        Ok(None)
    }
}

impl StatementRewrite<'_> {
    /// Rewrites non-deterministic functions like now() into either ParamRefs or correctly formatted Strings,
    /// for the purpose of maintaining consistency across databases for omni tables (and now() for sharded tables).
    pub(super) fn rewrite_nd_functions<'mem, 'mutref>(
        &mut self,
        mut stmt: NodeMut<'mem, 'mutref>,
        mem: MemoryToken<'mem>,
        bind_params: &mut BindParams,
    ) -> Result<(), Error> {
        if matches!(stmt.as_ref(), Node::InsertStmt(_)) {
            let mut parser = StatementParser::new(stmt.as_ref(), None, self.schema);

            let mut nd_rewrite = NDRewrite {
                rewrite: self,
                bind_params,
                mem,
                statement_type: StatementType::Insert,
            };

            // We allow `ShardedTable`s on a case-by-case basis (see `apply_rewrite_on_sharded_tables`)
            let is_sharded = parser.is_sharded(
                nd_rewrite.rewrite.db_schema,
                nd_rewrite.rewrite.user,
                nd_rewrite.rewrite.search_path,
            );

            let Some((relation, cols, not_covered_cols)) =
                nd_rewrite.rewrite.find_not_used_cols(&mut stmt, mem)
            else {
                return Ok(());
            };

            let insert_context = InsertContext {
                relation,
                cols,
                rewrite_case: if is_sharded {
                    RewriteCase::TransactionTimeFunction
                } else {
                    RewriteCase::OmnishardedInsert
                },
            };

            // 1. iterates through Schema to find DEFAULT columns
            // 2. adds the column to target list & all the values lists (ParamRef or String)
            nd_rewrite.handle_adding_defaults(&mut stmt, &not_covered_cols, &insert_context)?;

            // Replaces all non-deterministic function calls (ParamRef or String)
            nd_rewrite.transform_func_calls_in_insert(stmt, &insert_context)?;
        } else if matches!(stmt, NodeMut::SelectStmt(_) | NodeMut::UpdateStmt(_)) {
            // Still rewrite other kind of statements that use ND functions that
            // are reliant on transaction start time

            let statement_type = match stmt {
                NodeMut::SelectStmt(_) => StatementType::Select,
                NodeMut::UpdateStmt(_) => StatementType::Update,
                _ => panic!("only select and update covered"),
            };

            let mut nd_rewrite = NDRewrite {
                rewrite: self,
                bind_params,
                mem,
                statement_type,
            };

            let mut transform_tt = ReplaceTransactionTime {
                nd_rewrite: &mut nd_rewrite,
                outer_error: None,
            };

            match stmt {
                NodeMut::SelectStmt(select_stmt) => transform_tt.transform_select_stmt(select_stmt),
                NodeMut::UpdateStmt(update_stmt) => transform_tt.transform_update_stmt(update_stmt),
                _ => panic!("only select and update covered"),
            }

            return transform_tt.outer_error.map(Err).unwrap_or(Ok(()));
        }
        Ok(())
    }

    /// Fetch the table Relation, so that we can get the relevant Schema for each column.
    /// Fetch the list of columns that are DEFAULT (and not already covered)
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

        let relation = self.db_schema.table(table, self.user, self.search_path)?;
        let cols = insert_stmt.cols();

        // Find the columns that the insert does NOT cover.
        let not_covered_cols: Vec<String> = if !cols.is_empty() {
            let subset: Vec<&str> = cols
                .iter()
                .filter_map(|col| match col {
                    Node::ResTarget(target) => target.name(),
                    _ => None,
                })
                .collect();

            relation
                .column_names()
                .filter(|name| !subset.contains(name))
                .map(|name| name.to_string())
                .collect()
        } else {
            vec![]
        };

        Some((relation.clone(), mem.make_unique(cols), not_covered_cols))
    }
}

struct NDRewrite<'mem, 'a, 's> {
    rewrite: &'a mut StatementRewrite<'s>,
    bind_params: &'a mut BindParams,
    mem: MemoryToken<'mem>,
    statement_type: StatementType,
}

enum StatementType {
    Insert,
    Select,
    Update,
}

/// Extra context necessary for re-writing INSERT statements, where we need
/// information to determine things like if it's sharded, or what kind of column each column is
struct InsertContext<'mem> {
    relation: Relation,
    cols: Unique<'mem, &'mem NodeList>,
    rewrite_case: RewriteCase,
}

impl<'mem, 'a, 's> NDRewrite<'mem, 'a, 's> {
    /// Replaces all non-deterministic function calls (ParamRef or String)
    /// If simple protocol, make an A_Const node with the String constant of the formatted function output.
    /// If extended or prepare, make a ParamRef, so that we can cache it and put in the formatted output later.
    fn make_node(&mut self, nd_function: &NDFunction) -> Result<Unique<'mem, Node<'mem>>, Error> {
        self.rewrite.rewritten = true;

        Ok(if !self.rewrite.extended && !self.rewrite.prepared {
            let text = match nd_function
                .write_as_constant(&self.rewrite.query_timestamps, self.rewrite.timezone)
            {
                Ok((text, _)) => text,
                // The statement is discarded when the error is returned (thus, value doesn't matter)
                Err(err) => return Err(err),
            };

            let constant_text_node = self
                .mem
                .make_a_const(ConstValue::String(text.as_str()))
                .uncast();

            match self.statement_type {
                StatementType::Insert | StatementType::Update => constant_text_node,
                StatementType::Select => self
                    .mem
                    .make_type_cast(
                        constant_text_node,
                        self.mem.make_list(&[self
                            .mem
                            .make_string(Some(nd_function.col_type_to_type_cast_alias()))]),
                    )
                    .uncast(),
            }
            .uncast()
        } else {
            let param_ref = self.mem.make_param_ref(self.bind_params.len() as i32 + 1);

            // TODO: add a method to plan() for this...
            self.bind_params
                .push(BindParam::NDFunction(nd_function.clone()));

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
                        .make_string(Some(nd_function.col_type_to_type_cast_alias()))]),
                )
                .uncast()
        })
    }
}
