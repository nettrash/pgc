use crate::{
    comparer::core::Comparer,
    config::{core::Config, dump_config::DumpConfig, grants_mode::GrantsMode},
    dump::core::Dump,
};
use chrono::Datelike;
use clap::{CommandFactory, Parser};
use std::{io::Error, path::Path};

pub mod comparer;
pub mod config;
pub mod dump;

// Command line arguments.
#[derive(Parser, Debug)]
#[command(
    name = "pgc",
    author = "nettrash",
    version = env!("CARGO_PKG_VERSION"),
    about = "PostgreSQL Database Schema Comparer.",
    long_about = None,
)]
struct Args {
    /// Command to execute: dump or compare
    #[arg(long)]
    command: Option<String>,

    /// Hostname for the command
    #[arg(long, default_value = "localhost")]
    server: Option<String>,

    /// Hostname for the command
    #[arg(long, default_value = "5432")]
    port: Option<String>,

    /// User name for the command
    #[arg(long, default_value = "")]
    user: Option<String>,

    /// Password of user for the command
    #[arg(long, default_value = "")]
    password: Option<String>,

    /// Database name for the command
    #[arg(long, default_value = "postgres")]
    database: Option<String>,

    /// Schema name for the command
    #[arg(long, default_value = "public")]
    scheme: Option<String>,

    /// Output file name for the command
    #[arg(long, default_value = "data.out")]
    output: Option<String>,

    /// From file for comparison
    #[arg(long, default_value = "dump.from")]
    from: Option<String>,

    /// To file for comparison
    #[arg(long, default_value = "dump.to")]
    to: Option<String>,

    /// Configuration file for the comparer
    #[arg(long)]
    config: Option<String>,

    /// Use SSL for PostgreSQL connection
    #[arg(long)]
    use_ssl: bool,

    /// Use DROP statements in the output
    #[arg(long, default_value = "false")]
    use_drop: bool,

    /// True - if explicit begin...commit statement has to be added into resulting diff file; False - otherwise
    #[arg(long, default_value = "false")]
    use_single_transaction: bool,

    /// Include comments in the output script
    #[arg(long, default_value = "true")]
    use_comments: bool,

    /// Grants handling mode: ignore, addonly, full
    #[arg(long, default_value = "ignore")]
    grants_mode: String,
}

// Main entry point for the program.
#[tokio::main]
pub async fn main() -> Result<(), Error> {
    pgc_version();
    let args = Args::parse();
    if args.command.is_none() && args.config.is_none() {
        let mut cmd = Args::command();
        let _ = cmd.print_help();
        return Ok(());
    }
    if let Some(config) = args.config.clone() {
        println!("Using configuration file: {config}");
        return run_by_config(config).await;
    } else if let Some(command) = args.command.as_deref() {
        match command {
            "dump" => {
                println!("Dumping database...");
                return create_dump(DumpConfig {
                    host: args.server.unwrap(),
                    port: args.port.unwrap(),
                    user: args.user.unwrap(),
                    password: args.password.unwrap(),
                    database: args.database.unwrap(),
                    scheme: args.scheme.unwrap(),
                    ssl: args.use_ssl,
                    file: args.output.unwrap(),
                })
                .await;
            }
            "compare" => {
                println!("Comparing databases...");
                return compare_dumps(
                    args.from.unwrap(),
                    args.to.unwrap(),
                    args.output.unwrap(),
                    args.use_drop,
                    args.use_single_transaction,
                    args.use_comments,
                    GrantsMode::from_str_or_panic(&args.grants_mode),
                )
                .await;
            }
            _ => {
                eprintln!("Unknown command: {command}");
                return Ok(());
            }
        }
    }
    Ok(())
}

// Function to print the version information.
fn pgc_version() {
    let version = env!("CARGO_PKG_VERSION");
    println!("pgc v{version}");
    let year = chrono::Utc::now().year();
    println!("(c) 2025-{year} nettrash. All rights reserved.");
    println!("This program is licensed under the MIT License.");
    println!();
}

async fn run_by_config(config: String) -> Result<(), Error> {
    // Here you would read the config file and execute the appropriate command.
    // For now, we just print the config file name.
    if Path::new(&config).exists() {
        println!("Running with config: {config}");
        let cfg: Config = Config::new(config.clone());

        let from_file = cfg.from.file.clone();
        let to_file = cfg.to.file.clone();
        let output_file = cfg.output.clone();

        let result = create_dump(DumpConfig {
            host: cfg.from.host,
            port: cfg.from.port,
            user: cfg.from.user,
            password: cfg.from.password,
            database: cfg.from.database,
            scheme: cfg.from.scheme,
            ssl: cfg.from.ssl,
            file: from_file.clone(),
        })
        .await;
        if let Err(e) = result {
            eprintln!("Error creating dump: {e}");
            return Err(e);
        }
        let result = create_dump(DumpConfig {
            host: cfg.to.host,
            port: cfg.to.port,
            user: cfg.to.user,
            password: cfg.to.password,
            database: cfg.to.database,
            scheme: cfg.to.scheme,
            ssl: cfg.to.ssl,
            file: to_file.clone(),
        })
        .await;
        if let Err(e) = result {
            eprintln!("Error creating dump: {e}");
            return Err(e);
        }
        println!("Dumps created successfully. Now comparing...");

        let compare_result = compare_dumps(
            from_file,
            to_file,
            output_file,
            cfg.use_drop,
            cfg.use_single_transaction,
            cfg.use_comments,
            cfg.grants_mode,
        )
        .await;

        if let Err(e) = compare_result {
            eprintln!("Error comparing dumps: {e}");
            return Err(e);
        }
        Ok(())
    } else {
        eprintln!("Config file does not exist: {config}");
        Err(Error::new(
            std::io::ErrorKind::NotFound,
            "Config file not found",
        ))
    }
}

async fn create_dump(dump_config: DumpConfig) -> Result<(), Error> {
    let mut dump = Dump::new(dump_config);
    println!("Creating dump...");
    let result = dump.process().await;
    if let Err(e) = result {
        eprintln!("Error creating dump: {e}");
        return Err(e);
    }
    Ok(())
}

async fn compare_dumps(
    from: String,
    to: String,
    output: String,
    use_drop: bool,
    use_single_transaction: bool,
    use_comments: bool,
    grants_mode: GrantsMode,
) -> Result<(), Error> {
    println!("Reading dumps...");
    let from = Dump::read_from_file(&from).await?;
    let to = Dump::read_from_file(&to).await?;
    println!("--> Dump from:\n{}\n", from.get_info());
    println!("--> Dump to:\n{}\n", to.get_info());
    println!("Comparing dumps...");
    let mut comparer = Comparer::new(
        from,
        to,
        use_drop,
        use_single_transaction,
        use_comments,
        grants_mode,
    );
    comparer.compare().await?;
    comparer.save_script(&output).await?;
    println!("Dump compared successfully. Result script: {output}");
    Ok(())
}
