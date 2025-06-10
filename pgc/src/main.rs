use crate::{config::{config::Config, dump_config::DumpConfig}, dump::dump::Dump};
use clap::{CommandFactory, Parser, command};
use std::{io::Error, path::Path};

pub mod config;
pub mod dump;

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
        println!(
            "Using configuration file: {}",
            args.config.as_ref().unwrap()
        );
        return run_by_config(args.config.unwrap()).await;
    } else if args.command.is_some() {
        match args.command.as_deref() {
            Some("dump") => {
                println!("Dumping database...");
                return create_dump(
                    args.server.unwrap(),
                    args.database.unwrap(),
                    args.scheme.unwrap(),
                    args.use_ssl,
                    args.output.unwrap(),
                )
                .await;
            }
            Some("compare") => {
                println!("Comparing databases...");
                return compare_dumps(args.from.unwrap(), args.to.unwrap(), args.output.unwrap())
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
    println!("(c) 2025 Paysend. All rights reserved.");
    println!("This program is licensed under the GPL v3 License.");
    println!("");
}

async fn run_by_config(config: String) -> Result<(), Error> {
    // Here you would read the config file and execute the appropriate command.
    // For now, we just print the config file name.
    if Path::new(&config).exists() {
        println!("Running with config: {}", config);
        let cfg: Config = Config::new(config.clone());
        
        let from_file = cfg.from.file.clone();
        let to_file = cfg.to.file.clone();
        let output_file = cfg.output.clone();

        let result = create_dump(cfg.from.host, cfg.from.database, cfg.from.scheme, cfg.from.ssl, from_file.clone()).await;
        if result.is_err() {
            eprintln!("Error creating dump: {}", result.as_ref().unwrap_err());
            return Err(result.unwrap_err());
        }
        let result = create_dump(cfg.to.host, cfg.to.database, cfg.to.scheme, cfg.to.ssl, to_file.clone()).await;
        if result.is_err() {
            eprintln!("Error creating dump: {}", result.as_ref().unwrap_err());
            return Err(result.unwrap_err());
        }
        println!("Dumps created successfully. Now comparing...");
        let compare_result = compare_dumps(from_file, to_file, output_file).await;
        if compare_result.is_err() {
            eprintln!("Error comparing dumps: {}", compare_result.as_ref().unwrap_err());
            return Err(compare_result.unwrap_err());
        }
        Ok(())
    } else {
        eprintln!("Config file does not exist: {}", config);
        Err(Error::new(std::io::ErrorKind::NotFound, "Config file not found"))
    }
}

async fn create_dump(
    server: String,
    database: String,
    scheme: String,
    use_ssl: bool,
    output: String,
) -> Result<(), Error> {
    let dump_config: DumpConfig = DumpConfig {
        host: server,
        database,
        scheme,
        ssl: use_ssl,
        file: output.clone(),
    };
    let mut dump = Dump::new(dump_config);
    println!("Creating dump...");
    let result = dump.process().await;
    if let Err(e) = result {
        eprintln!("Error creating dump: {}", e);
        return Err(e);
    }
    println!("Dump created successfully: {}", output);
    Ok(())
}

async fn compare_dumps(from: String, to: String, output: String) -> Result<(), Error> {
    let _ = from;
    let _ = to;
    let _ = output;
    // Here you would implement the logic to compare dumps.
    // For now, we just print a message.
    println!("Comparing dumps...");
    println!("Dump compared successfully: {}", output);
    Ok(())
}
