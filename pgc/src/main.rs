use clap::{command, CommandFactory, Parser};
use std::{io::Error, path::Path};

use crate::config::config::Config;

pub mod config;

// Command line arguments.
#[derive(Parser, Debug)]
#[command(
    name = "pgc",
    author = "Paysend",
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
        println!("Using configuration file: {}", args.config.as_ref().unwrap());
        return run_by_config(args.config.unwrap()).await;
    } else if args.command.is_some() {
        match args.command.as_deref() {
            Some("dump") => {
                println!("Dumping database...");
                return create_dump(args.server.unwrap(), args.database.unwrap(), args.scheme.unwrap(), args.use_ssl, args.output.unwrap()).await;
            }
            Some("compare") => {
                println!("Comparing databases...");
                return compare_dumps(args.from.unwrap(), args.to.unwrap(), args.output.unwrap()).await;
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
    println!("(c) 2025 Paysend. All rights reserved.");
    println!("This program is licensed under the GPL v3 License.");
    println!("");
}

async fn run_by_config(config: String) -> Result<(), Error> {
    // Here you would read the config file and execute the appropriate command.
    // For now, we just print the config file name.
    Path::new(&config).exists().then(|| {
        println!("Running with config: {}", config);
        let cfg: Config = Config::new(config.clone());
        println!("Configuration: {:?}", cfg);
    }).unwrap_or_else(|| {
        eprintln!("Config file does not exist: {}", config);
    });
    Ok(())
}

async fn create_dump(server: String, database: String, scheme: String, use_ssl: bool, output: String) -> Result<(), Error> {
    let _ = server;
    let _ = database;
    let _ = scheme;
    let _ = use_ssl;
    let _ = output;
    // Here you would implement the logic to create a dump of the database.
    // For now, we just print a message.
    println!("Creating dump...");
    Ok(())
}

async fn compare_dumps(from: String, to: String, output: String) -> Result<(), Error> {
    let _ = from;
    let _ = to;
    let _ = output;
    // Here you would implement the logic to compare dumps.
    // For now, we just print a message.
    println!("Comparing dumps...");
    Ok(())
}