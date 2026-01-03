use std::path::PathBuf;

/// Resolved configuration with all values filled in (no Options).
///
/// This struct is created by resolving precedence: CLI args > env vars > defaults.
/// All fields have concrete values, making it safe to access directly without unwrapping.
#[derive(Debug, Clone)]
pub struct ResolvedConfig {
    // Paths
    pub download_dir_mc: PathBuf,
    pub download_dir_pt: PathBuf,
    pub parquet_dir_mc: PathBuf,
    pub parquet_dir_pt: PathBuf,

    // Processing
    pub batch_size: usize,
    pub max_retries: u32,
    pub retry_initial_delay_ms: u64,
    pub retry_max_delay_ms: u64,

    // Downloads
    pub concurrent_downloads: usize,
}

impl Default for ResolvedConfig {
    fn default() -> Self {
        Self {
            download_dir_mc: PathBuf::from("data/tmp/mc"),
            download_dir_pt: PathBuf::from("data/tmp/pt"),
            parquet_dir_mc: PathBuf::from("data/parquet/mc"),
            parquet_dir_pt: PathBuf::from("data/parquet/pt"),
            batch_size: 100,
            max_retries: 3,
            retry_initial_delay_ms: 1000,
            retry_max_delay_ms: 10000,
            concurrent_downloads: 4,
        }
    }
}

impl ResolvedConfig {
    /// Creates a `ResolvedConfig` from CLI arguments and environment variables.
    ///
    /// Precedence: CLI args > env vars > defaults
    ///
    /// # Arguments
    ///
    /// * `cli_batch_size` - Optional batch size from CLI arguments (highest precedence)
    ///
    /// # Returns
    ///
    /// A `ResolvedConfig` with all values resolved according to precedence rules.
    pub fn from_cli_and_env(cli_batch_size: Option<usize>) -> Self {
        // Resolve batch_size: CLI > env > default
        let batch_size = cli_batch_size
            .or_else(|| {
                std::env::var("SPPD_BATCH_SIZE")
                    .ok()
                    .and_then(|v| v.parse().ok())
            })
            .unwrap_or(100);

        Self {
            // Paths - always use defaults (no config file support)
            download_dir_mc: PathBuf::from("data/tmp/mc"),
            download_dir_pt: PathBuf::from("data/tmp/pt"),
            parquet_dir_mc: PathBuf::from("data/parquet/mc"),
            parquet_dir_pt: PathBuf::from("data/parquet/pt"),

            // Processing - use resolved batch_size, defaults for others
            batch_size,
            max_retries: 3,
            retry_initial_delay_ms: 1000,
            retry_max_delay_ms: 10000,

            // Downloads - use defaults
            concurrent_downloads: 4,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn default_config_values() {
        let config = ResolvedConfig::default();
        assert_eq!(config.batch_size, 100);
        assert_eq!(config.concurrent_downloads, 4);
    }

    #[test]
    fn from_cli_overrides_batch_size() {
        let config = ResolvedConfig::from_cli_and_env(Some(25));
        assert_eq!(config.batch_size, 25);
    }
}
