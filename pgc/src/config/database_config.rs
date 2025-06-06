// This is a database dump configuration structure.
#[derive(Debug, Clone)]
pub struct DumpConfig {
    // Database host
    pub host: String,
    // Database name
    pub database: String,
    // Schema name. Mask allowed. For example: sche*
    pub scheme: String,
    // Flag of SSL usage
    pub ssl: bool,
    // Dump file name
    pub file: String,
}