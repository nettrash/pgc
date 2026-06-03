use crate::config::dump_config::DumpConfig;
use crate::config::grants_mode::GrantsMode;

// Configuration file representation.
#[derive(Debug, Clone)]
pub struct Config {
    // From Dump Configuration
    pub from: DumpConfig,
    // To Dump Configuration
    pub to: DumpConfig,
    // Output file name for the comparison result
    pub output: String,
    // Whether to use DROP statements in the output
    pub use_drop: bool,
    // True - if explicit begin...commit statement has to be added into resulting diff file; False - otherwise
    pub use_single_transaction: bool,
    // Whether to include comments in the output script
    pub use_comments: bool,
    // How to handle grants (privileges) during comparison
    pub grants_mode: GrantsMode,
    // Maximum number of connections in the PostgreSQL connection pool
    pub max_connections: u32,
    // Whether to emit a migration script that is safe/convenient to run on a
    // live production database (concurrent index builds, partition-aware index
    // creation, NOT VALID + VALIDATE for foreign keys, concurrent index drops,
    // and a split transaction so the concurrent statements run outside it).
    pub output_for_production: bool,
}

impl Config {
    /// Load configuration from `file`. Returns a descriptive error instead of
    /// panicking on malformed input so callers can format it however they want.
    pub fn load(file: &str) -> Result<Self, String> {
        let binding = std::fs::read_to_string(file)
            .map_err(|e| format!("Error reading configuration file {file}: {e}"))?;
        let config_data = binding
            .split('\n')
            .filter(|line| !line.trim().is_empty() && !line.trim().starts_with('#'))
            .collect::<Vec<&str>>();

        let mut from_host = "".to_string();
        let mut from_port = "5432".to_string();
        let mut from_user = "".to_string();
        let mut from_password = "".to_string();
        let mut from_database = "".to_string();
        let mut from_scheme = "".to_string();
        let mut from_ssl = false;
        let mut from_dump = "dump.from".to_string();
        let mut to_host = "".to_string();
        let mut to_port = "5432".to_string();
        let mut to_user = "".to_string();
        let mut to_password = "".to_string();
        let mut to_database = "".to_string();
        let mut to_scheme = "".to_string();
        let mut to_ssl = false;
        let mut to_dump = "dump.to".to_string();
        let mut output = "data.out".to_string();
        let mut use_drop = false;
        let mut use_single_transaction = false;
        let mut use_comments = true;
        let mut grants_mode = GrantsMode::Ignore;
        let mut max_connections: u32 = 16;
        let mut output_for_production = false;

        for line in &config_data {
            if line.trim().is_empty() || line.starts_with('#') {
                continue; // Skip empty lines and comments
            }
            let parts: Vec<&str> = line.splitn(2, '=').collect();
            if parts.len() != 2 || parts[0].trim().is_empty() || parts[1].trim().is_empty() {
                return Err(format!("Invalid configuration line: {line}"));
            }
            let key = parts[0].trim().to_uppercase();
            let value = parts[1].trim().to_uppercase();
            let raw_value = parts[1].trim();
            if key != "FROM_HOST"
                && key != "FROM_PORT"
                && key != "FROM_USER"
                && key != "FROM_PASSWORD"
                && key != "FROM_DATABASE"
                && key != "FROM_SCHEME"
                && key != "FROM_SSL"
                && key != "FROM_DUMP"
                && key != "TO_HOST"
                && key != "TO_PORT"
                && key != "TO_USER"
                && key != "TO_PASSWORD"
                && key != "TO_DATABASE"
                && key != "TO_SCHEME"
                && key != "TO_SSL"
                && key != "TO_DUMP"
                && key != "OUTPUT"
                && key != "USE_DROP"
                && key != "USE_SINGLE_TRANSACTION"
                && key != "USE_COMMENTS"
                && key != "GRANTS_MODE"
                && key != "MAX_CONNECTIONS"
                && key != "OUTPUT_FOR_PRODUCTION"
            {
                return Err(format!("Unknown configuration key: {}", parts[0]));
            }
            if key == "FROM_SSL" && value != "TRUE" && value != "FALSE" {
                return Err(format!("Invalid value for FROM_SSL: {raw_value}"));
            }
            if key == "TO_SSL" && value != "TRUE" && value != "FALSE" {
                return Err(format!("Invalid value for TO_SSL: {raw_value}"));
            }

            match key.as_str() {
                "FROM_HOST" => from_host = raw_value.to_string(),
                "FROM_PORT" => from_port = raw_value.to_string(),
                "FROM_USER" => from_user = raw_value.to_string(),
                "FROM_PASSWORD" => from_password = raw_value.to_string(),
                "FROM_DATABASE" => from_database = raw_value.to_string(),
                "FROM_SCHEME" => from_scheme = raw_value.to_string(),
                "FROM_SSL" => from_ssl = value == "TRUE",
                "FROM_DUMP" => from_dump = raw_value.to_string(),
                "TO_HOST" => to_host = raw_value.to_string(),
                "TO_PORT" => to_port = raw_value.to_string(),
                "TO_USER" => to_user = raw_value.to_string(),
                "TO_PASSWORD" => to_password = raw_value.to_string(),
                "TO_DATABASE" => to_database = raw_value.to_string(),
                "TO_SCHEME" => to_scheme = raw_value.to_string(),
                "TO_SSL" => to_ssl = value == "TRUE",
                "TO_DUMP" => to_dump = raw_value.to_string(),
                "OUTPUT" => output = raw_value.to_string(),
                "USE_DROP" => use_drop = value == "TRUE",
                "USE_SINGLE_TRANSACTION" => use_single_transaction = value == "TRUE",
                "OUTPUT_FOR_PRODUCTION" => {
                    output_for_production = match value.as_str() {
                        "TRUE" => true,
                        "FALSE" => false,
                        _ => {
                            return Err(format!(
                                "Invalid value for OUTPUT_FOR_PRODUCTION: {raw_value}"
                            ));
                        }
                    };
                }
                "USE_COMMENTS" => {
                    use_comments = match value.as_str() {
                        "TRUE" => true,
                        "FALSE" => false,
                        _ => return Err(format!("Invalid value for USE_COMMENTS: {raw_value}")),
                    };
                }
                "GRANTS_MODE" => {
                    grants_mode = raw_value.parse::<GrantsMode>().map_err(|e| e.to_string())?;
                }
                "MAX_CONNECTIONS" => {
                    let v = raw_value
                        .parse::<u32>()
                        .map_err(|e| format!("Invalid value for MAX_CONNECTIONS: {e}"))?;
                    if v < 1 {
                        return Err(format!("MAX_CONNECTIONS must be at least 1, got {v}"));
                    }
                    max_connections = v;
                }
                _ => {}
            }
        }
        let from = DumpConfig {
            host: from_host,
            port: from_port,
            user: from_user,
            password: from_password,
            database: from_database,
            scheme: from_scheme,
            ssl: from_ssl,
            file: from_dump,
        };
        let to = DumpConfig {
            host: to_host,
            port: to_port,
            user: to_user,
            password: to_password,
            database: to_database,
            scheme: to_scheme,
            ssl: to_ssl,
            file: to_dump,
        };

        // A FROM and TO that point at exactly the same database+schema almost
        // always indicates a typo in the config file — comparing a DB to
        // itself produces an empty diff, which silently looks like a
        // successful run. Warn loudly so the user notices.
        if !from.host.is_empty()
            && !to.host.is_empty()
            && from.host.eq_ignore_ascii_case(&to.host)
            && from.port == to.port
            && from.database.eq_ignore_ascii_case(&to.database)
            && from.scheme == to.scheme
        {
            eprintln!(
                "Warning: FROM and TO point at the same target \
                 (host={}, port={}, database={}, scheme={}). The comparison \
                 will produce an empty diff. Double-check the config file.",
                from.host, from.port, from.database, from.scheme
            );
        }

        // An empty FROM/TO host slips past the same-target check above
        // (which gates on both hosts being non-empty). A blank host almost
        // always means the user forgot a key, mistyped one, or fed in an
        // empty config file by mistake; silently falling back to defaults
        // produces a baffling connection error later. Surface it now.
        if from.host.is_empty() {
            eprintln!(
                "Warning: FROM_HOST is empty in the configuration file — \
                 connection will likely fail. Check that the FROM_* keys are set."
            );
        }
        if to.host.is_empty() {
            eprintln!(
                "Warning: TO_HOST is empty in the configuration file — \
                 connection will likely fail. Check that the TO_* keys are set."
            );
        }

        Ok(Config {
            from,
            to,
            output,
            use_drop,
            use_single_transaction,
            use_comments,
            grants_mode,
            max_connections,
            output_for_production,
        })
    }

    /// Back-compat shim for existing call sites that expect a panicking constructor.
    /// New code should prefer `Config::load`.
    pub fn new(file: String) -> Self {
        match Self::load(&file) {
            Ok(cfg) => cfg,
            Err(e) => panic!("{e}"),
        }
    }
}

#[cfg(test)]
#[path = "core_tests.rs"]
mod tests;
