use std::path::PathBuf;

use clap::{Parser, Subcommand};
use tracing::info;

use crate::api::resharding::ReshardTask;
use crate::api::schema_sync::{SchemaSyncPhase, SchemaSyncTask};
use crate::api::task::TaskError;
use crate::api::{MigrationError, run_task};
use crate::backend::databases::databases;
use crate::backend::replication::logical::Error::ReplicationAborted;
use crate::backend::replication::logical::table_move::TableMove;
use crate::backend::replication::resharding_state::ReshardingState;
use crate::backend::schema::sync::config::ShardConfig;
use crate::frontend::comms::comms;
use crate::frontend::router::cli::RouterCli;
use pgdog_stats::Databases;

/// PgDog is a PostgreSQL pooler, proxy, load balancer and query router.
#[derive(Parser, Debug)]
#[command(name = "", version = concat!("PgDog v", env!("GIT_HASH")))]
pub(crate) struct Cli {
    /// Path to the configuration file. Default: "pgdog.toml"
    #[arg(short, long, default_value = "pgdog.toml")]
    pub(crate) config: PathBuf,
    /// Path to the users.toml file. Default: "users.toml"
    #[arg(short, long, default_value = "users.toml")]
    pub(crate) users: PathBuf,
    /// Connection URL.
    #[arg(short, long)]
    pub(crate) database_url: Option<Vec<String>>,
    /// Subcommand.
    #[command(subcommand)]
    pub(crate) command: Option<Commands>,
}

#[derive(Subcommand, Debug, Clone)]
pub(crate) enum Commands {
    /// Start PgDog.
    Run {
        /// Size of the connection pool.
        #[arg(short, long)]
        pool_size: Option<usize>,

        /// Minimum number of idle connections to maintain open.
        #[arg(short, long)]
        min_pool_size: Option<usize>,

        /// Run the pooler in session mode.
        #[arg(short, long)]
        session_mode: Option<bool>,
    },

    /// Generate a SCRAM-SHA-256 hash from a plaintext password.
    /// Output can be stored directly in users.toml.
    Hash {
        /// The plaintext password to hash.
        password: String,
    },

    /// Execute the router on the queries.
    Route {
        /// User in users.toml.
        #[arg(short, long)]
        user: String,

        /// Database in pgdog.toml.
        #[arg(short, long)]
        database: String,

        /// Path to the file containing the queries.
        #[arg(short, long)]
        file: PathBuf,
    },

    /// Check configuration files for errors.
    Configcheck,

    /// Copy data from source to destination cluster
    /// using logical replication.
    DataSync {
        /// Source database name.
        #[arg(long)]
        from_database: String,

        /// Publication name.
        #[arg(long, required_unless_present = "table", conflicts_with = "table")]
        publication: Option<String>,

        /// Move a single table ("table" or "schema.table") instead of a publication.
        /// Schema sync creates only this table. Runs until interrupted, without cutover.
        #[arg(long)]
        table: Option<String>,

        /// Destination database.
        #[arg(long)]
        to_database: String,

        /// Only replicate; skip the initial data copy.
        #[arg(long, default_value = "false", conflicts_with = "table")]
        replicate_only: bool,

        /// Only copy data and synchronize tables; skip replication and cutover.
        #[arg(long, default_value = "false")]
        sync_only: bool,

        /// Name of the replication slot to create/use.
        #[arg(long)]
        replication_slot: Option<String>,

        /// Don't perform pre-data or post-data schema sync.
        #[arg(long)]
        skip_schema_sync: bool,
    },

    /// Copy schema from source to destination cluster.
    SchemaSync {
        /// Source database name.
        #[arg(long)]
        from_database: String,
        /// Publication name.
        #[arg(long)]
        publication: String,

        /// Destination database.
        #[arg(long)]
        to_database: String,

        /// Dry run. Print schema commands, don't actually execute them.
        #[arg(long)]
        dry_run: bool,

        /// Ignore errors for any phase. Post-data ignores errors by default.
        #[arg(long)]
        ignore_errors: bool,

        #[arg(
            long,
            value_enum,
            help = "Schema sync phase to run.",
            default_value_t = SchemaSyncPhase::Pre,
            conflicts_with_all = ["data_sync_complete", "cutover", "validation"]
        )]
        phase: SchemaSyncPhase,

        #[arg(long, hide = true, conflicts_with_all = ["cutover", "validation"])]
        data_sync_complete: bool,

        #[arg(long, hide = true, conflicts_with = "validation")]
        cutover: bool,

        #[arg(long, hide = true)]
        validation: bool,
    },

    /// For testing purposes only.
    ///
    /// Performs the entire schema sync, data sync and replication flow
    /// with cutover trigger.
    ///
    /// Use for internal testing only. To do this in production,
    /// use the admin database RESHARD command.
    ///
    ReplicateAndCutover {
        /// Source database name.
        #[arg(long)]
        from_database: String,

        /// Destination database name.
        #[arg(long)]
        to_database: String,

        /// Publication name.
        #[arg(long)]
        publication: String,

        /// Replication slot name.
        #[arg(long)]
        replication_slot: Option<String>,
    },

    /// Perform cluster configuration steps
    /// required for sharded operations.
    Setup {
        /// Database name.
        #[arg(long)]
        database: String,
    },
}

/// Generate and print a SCRAM-SHA-256 hash from a plaintext password.
#[allow(clippy::print_stdout)]
pub(crate) fn hash_password(password: &str) {
    use rand::Rng;

    let salt: [u8; 16] = rand::rng().random();
    let iterations = std::num::NonZeroU32::new(4096).unwrap();
    println!(
        "{}",
        crate::auth::scram::generate_hash(password, iterations, &salt)
    );
}

/// FOR TESTING PURPOSES ONLY.
pub(crate) async fn replicate_and_cutover(
    commands: Commands,
) -> Result<(), Box<dyn std::error::Error>> {
    if let Commands::ReplicateAndCutover {
        from_database,
        to_database,
        publication,
        replication_slot,
    } = commands
    {
        let state = ReshardingState::builder()
            .source(&from_database)
            .destination(&to_database)
            .publication(&publication)
            .maybe_replication_slot(replication_slot.clone())
            .build()?;

        run_task(
            ReshardTask::builder()
                .state(state)
                .auto_cutover(true)
                .build(),
        )
        .await?;
    }

    Ok(())
}

pub(crate) async fn data_sync(commands: Commands) -> Result<(), Box<dyn std::error::Error>> {
    if let Commands::DataSync {
        from_database,
        to_database,
        publication,
        table,
        replicate_only,
        sync_only,
        replication_slot,
        skip_schema_sync,
    } = commands
    {
        let table_move = table.map(TableMove::new);
        let publication = match (&table_move, publication) {
            (Some(table_move), _) => table_move.publication().to_owned(),
            (None, Some(publication)) => publication,
            (None, None) => return Err("--publication or --table is required".into()),
        };

        let state = ReshardingState::builder()
            .source(&from_database)
            .destination(&to_database)
            .publication(&publication)
            .maybe_replication_slot(replication_slot)
            .build()?;

        let result = run_task(
            ReshardTask::builder()
                .state(state)
                .maybe_table_move(table_move)
                .skip_schema_sync(skip_schema_sync)
                .replicate_only(replicate_only)
                .sync_only(sync_only)
                .build(),
        )
        .await;

        if let Err(TaskError::Failed(MigrationError::Replication(ReplicationAborted))) = result
            && comms().offline()
        {
            // pgdog is shutting down, that's why the replication process is aborted.
            // Consider this as a success replication and other errors as fail.
            info!("replication stopped due to shutdown");
        } else {
            result?;
        }
    }

    Ok(())
}

fn legacy_phase(
    data_sync_complete: bool,
    cutover: bool,
    validation: bool,
) -> Option<SchemaSyncPhase> {
    if data_sync_complete {
        Some(SchemaSyncPhase::Post)
    } else if cutover {
        Some(SchemaSyncPhase::Cutover)
    } else if validation {
        Some(SchemaSyncPhase::PostDataValidation)
    } else {
        None
    }
}

pub(crate) async fn schema_sync(commands: Commands) -> Result<(), Box<dyn std::error::Error>> {
    if let Commands::SchemaSync {
        from_database,
        to_database,
        publication,
        dry_run,
        ignore_errors,
        phase,
        data_sync_complete,
        cutover,
        validation,
    } = commands
    {
        let phase = legacy_phase(data_sync_complete, cutover, validation).unwrap_or(phase);
        run_task(
            SchemaSyncTask::builder()
                .databases(Databases {
                    source: from_database,
                    destination: to_database,
                })
                .publication(publication)
                .phase(phase)
                .ignore_errors(ignore_errors || phase == SchemaSyncPhase::Post)
                .dry_run(dry_run)
                .build(),
        )
        .await?;
    } else {
        return Ok(());
    }

    Ok(())
}

pub(crate) async fn setup(database: &str) -> Result<(), Box<dyn std::error::Error>> {
    let databases = databases();
    let schema_owner = databases.schema_owner(database)?;

    ShardConfig::sync_all(&schema_owner).await?;

    Ok(())
}

pub(crate) async fn route(commands: Commands) -> Result<(), Box<dyn std::error::Error>> {
    if let Commands::Route {
        user,
        database,
        file,
    } = commands
    {
        let cli = RouterCli::new(&database, &user, file).await?;
        let cmds = cli.run()?;

        for cmd in cmds {
            info!("{:?}", cmd);
        }
    }

    Ok(())
}

#[cfg(test)]
mod test {
    mod data_sync {
        use crate::cli::*;

        fn parse(args: &[&str]) -> Result<Commands, clap::Error> {
            let mut argv = vec![
                "pgdog",
                "data-sync",
                "--from-database",
                "a",
                "--to-database",
                "b",
            ];
            argv.extend_from_slice(args);
            Ok(Cli::try_parse_from(argv)?
                .command
                .expect("data-sync is a command"))
        }

        #[test]
        fn test_table_or_publication() {
            let Commands::DataSync {
                table, publication, ..
            } = parse(&["--table", "sales.orders"]).unwrap()
            else {
                panic!("not a data sync command");
            };
            assert_eq!(table.as_deref(), Some("sales.orders"));
            assert_eq!(publication, None);

            let Commands::DataSync {
                table, publication, ..
            } = parse(&["--publication", "p"]).unwrap()
            else {
                panic!("not a data sync command");
            };
            assert_eq!(table, None);
            assert_eq!(publication.as_deref(), Some("p"));

            for args in [
                vec![],
                vec!["--table", "t", "--publication", "p"],
                vec!["--table", "t", "--replicate-only"],
            ] {
                assert!(parse(&args).is_err(), "{args:?}");
            }
        }
    }

    mod schema_sync {
        use crate::cli::*;

        fn parse(args: &[&str]) -> Result<SchemaSyncPhase, clap::Error> {
            let mut argv = vec![
                "pgdog",
                "schema-sync",
                "--from-database",
                "a",
                "--to-database",
                "b",
                "--publication",
                "p",
            ];
            argv.extend_from_slice(args);

            let Some(Commands::SchemaSync {
                phase,
                data_sync_complete,
                cutover,
                validation,
                ..
            }) = Cli::try_parse_from(argv)?.command
            else {
                panic!("not a schema sync command");
            };

            Ok(legacy_phase(data_sync_complete, cutover, validation).unwrap_or(phase))
        }

        #[test]
        fn test_phase_flags() {
            for (args, expected) in [
                (vec![], Some(SchemaSyncPhase::Pre)),
                (vec!["--phase", "post"], Some(SchemaSyncPhase::Post)),
                (vec!["--phase", "cutover"], Some(SchemaSyncPhase::Cutover)),
                (
                    vec!["--phase", "post-data-validation"],
                    Some(SchemaSyncPhase::PostDataValidation),
                ),
                (vec!["--data-sync-complete"], Some(SchemaSyncPhase::Post)),
                (vec!["--cutover"], Some(SchemaSyncPhase::Cutover)),
                (
                    vec!["--validation"],
                    Some(SchemaSyncPhase::PostDataValidation),
                ),
                (vec!["--phase", "bogus"], None),
                (vec!["--phase", "post", "--cutover"], None),
                (vec!["--cutover", "--validation"], None),
                (vec!["--data-sync-complete", "--validation"], None),
            ] {
                assert_eq!(parse(&args).ok(), expected, "{args:?}");
            }
        }
    }
}
