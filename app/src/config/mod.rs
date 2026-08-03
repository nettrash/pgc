//! Configuration types shared by the CLI and the `pgc.conf` file.
//!
//! - [`core`] — [`Config`](core::Config), the `KEY=VALUE` parser for `pgc.conf`.
//! - [`dump_config`] — [`DumpConfig`](dump_config::DumpConfig), the connection
//!   details for one side of a comparison.
//! - [`grants_mode`] — [`GrantsMode`](grants_mode::GrantsMode), the
//!   `ignore` / `addonly` / `full` privilege-handling selector.
//!
//! `--config <file>` takes precedence over every other flag and runs the full
//! chain: dump `FROM`, dump `TO`, then compare.

pub mod core;
pub mod dump_config;
pub mod grants_mode;
