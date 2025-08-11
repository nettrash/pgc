use crate::{
    comparer::core::Comparer,
    config::{core::Config, dump_config::DumpConfig},
    dump::core::Dump,
};
use clap::{CommandFactory, Parser, command};
use std::{io::Error, path::Path};

pub mod comparer;
pub mod config;
pub mod dump;

// Command line arguments.
#[derive(Parser, Debug)]
#[command(
    name = "pgc",
    author = "nettrash",
    version = "1.0.0",
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
    #[arg(long, default_value = false)]
    use_drop: bool,
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
    if args.config.is_some() {
        println!(
            "Using configuration file: {}",
            args.config.as_ref().unwrap()
        );
        return run_by_config(args.config.unwrap()).await;
    } else if args.command.is_some() {
        match args.command.as_deref() {
            Some("dump") => {
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
            Some("compare") => {
                println!("Comparing databases...");
                return compare_dumps(args.from.unwrap(), args.to.unwrap(), args.output.unwrap(), args.use_drop)
                    .await;
            }
            _ => {
                eprintln!("Unknown command: {}", args.command.unwrap());
                return Ok(());
            }
        }
    }
    Ok(())
}

// Function to print the version information.
fn pgc_version() {
    println!("pgc v1.0.0");
    println!("(c) 2025 nettrash. All rights reserved.");
    println!("This program is licensed under the GPL v3 License.");
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
        let use_drop = cfg.use_drop;

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
        let compare_result = compare_dumps(from_file, to_file, output_file, use_drop).await;
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

async fn compare_dumps(from: String, to: String, output: String, use_drop: bool) -> Result<(), Error> {
    println!("Reading dumps...");
    let from = Dump::read_from_file(&from).await?;
    let to = Dump::read_from_file(&to).await?;
    println!("--> Dump from:\n{}\n", from.get_info());
    println!("--> Dump to:\n{}\n", to.get_info());
    println!("Comparing dumps...");
    let mut comparer = Comparer::new(from, to, use_drop);
    comparer.compare().await?;
    comparer.save_script(&output).await?;
    println!("Dump compared successfully. Result script: {output}");
    Ok(())
}
