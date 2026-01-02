use crate::errors::{AppError, AppResult};
use serde::Deserialize;
use std::path::{Path, PathBuf};

/// Configuration structure for the application.
///
/// Supports loading from TOML files with optional sections.
/// Missing sections or fields will use default values.
#[derive(Debug, Default, Clone, Deserialize)]
#[serde(default)]
pub struct Config {
    pub paths: PathsConfig,
    pub processing: ProcessingConfig,
    pub downloads: DownloadsConfig,
}

/// Path configuration section.
#[derive(Debug, Clone, Default, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PathsConfig {
    pub base_dir: Option<String>,
    pub download_dir_mc: Option<String>,
    pub download_dir_pt: Option<String>,
    pub parquet_dir_mc: Option<String>,
    pub parquet_dir_pt: Option<String>,
}

/// Processing configuration section.
#[derive(Debug, Clone, Default, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ProcessingConfig {
    pub batch_size: Option<usize>,
    pub max_retries: Option<u32>,
    pub retry_initial_delay_ms: Option<u64>,
    pub retry_max_delay_ms: Option<u64>,
}

/// Downloads configuration section.
#[derive(Debug, Clone, Default, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DownloadsConfig {
    pub concurrent_downloads: Option<usize>,
}

/// Resolved configuration with all values filled in (no Options).
///
/// This struct is created by resolving precedence: CLI args > config file > env vars > defaults.
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

impl Config {
    /// Loads configuration from a TOML file.
    ///
    /// # Arguments
    ///
    /// * `path` - Path to the TOML configuration file
    ///
    /// # Returns
    ///
    /// Returns `Ok(Config)` if the file exists and is valid TOML.
    /// Returns an error if the file cannot be read or parsed.
    pub fn from_file<P: AsRef<Path>>(path: P) -> AppResult<Self> {
        let contents = std::fs::read_to_string(path.as_ref()).map_err(|e| {
            AppError::IoError(format!(
                "Failed to read config file {}: {e}",
                path.as_ref().display()
            ))
        })?;

        toml::from_str(&contents)
            .map_err(|e| AppError::ParseError(format!("Failed to parse config file: {e}")))
    }

    /// Attempts to load configuration from standard locations.
    ///
    /// Checks in order:
    /// 1. Current directory: `sppd.toml`
    /// 2. User config directory: `~/.config/sppd-cli/sppd.toml`
    ///
    /// Returns `Ok(None)` if no config file is found (not an error).
    /// Returns `Ok(Some(Config))` if a valid config file is found.
    /// Returns an error if a config file exists but cannot be parsed.
    pub fn from_standard_locations() -> AppResult<Option<Self>> {
        // Check current directory first
        let current_dir_config = PathBuf::from("sppd.toml");
        if current_dir_config.exists() {
            return Ok(Some(Self::from_file(&current_dir_config)?));
        }

        // Check user config directory
        if let Some(home) = dirs::home_dir() {
            let user_config = home.join(".config").join("sppd-cli").join("sppd.toml");
            if user_config.exists() {
                return Ok(Some(Self::from_file(&user_config)?));
            }
        }

        Ok(None)
    }

    /// Resolves configuration values with precedence: CLI args > config file > env vars > defaults.
    ///
    /// This method consolidates all precedence logic in one place, producing a `ResolvedConfig`
    /// with all values filled in. This eliminates the need for complex unwrapping throughout the codebase.
    ///
    /// # Arguments
    ///
    /// * `cli_batch_size` - Optional batch size from CLI arguments (highest precedence)
    ///
    /// # Returns
    ///
    /// A `ResolvedConfig` with all values resolved according to precedence rules.
    pub fn resolve(self, cli_batch_size: Option<usize>) -> ResolvedConfig {
        // Resolve batch_size: CLI > config > env > default
        let batch_size = cli_batch_size
            .or(self.processing.batch_size)
            .or_else(|| {
                std::env::var("SPPD_BATCH_SIZE")
                    .ok()
                    .and_then(|v| v.parse().ok())
            })
            .unwrap_or(100);

        ResolvedConfig {
            // Paths - use config values if present, otherwise defaults
            download_dir_mc: self
                .paths
                .download_dir_mc
                .map(PathBuf::from)
                .unwrap_or_else(|| PathBuf::from("data/tmp/mc")),
            download_dir_pt: self
                .paths
                .download_dir_pt
                .map(PathBuf::from)
                .unwrap_or_else(|| PathBuf::from("data/tmp/pt")),
            parquet_dir_mc: self
                .paths
                .parquet_dir_mc
                .map(PathBuf::from)
                .unwrap_or_else(|| PathBuf::from("data/parquet/mc")),
            parquet_dir_pt: self
                .paths
                .parquet_dir_pt
                .map(PathBuf::from)
                .unwrap_or_else(|| PathBuf::from("data/parquet/pt")),

            // Processing - use config values if present, otherwise defaults
            batch_size,
            max_retries: self.processing.max_retries.unwrap_or(3),
            retry_initial_delay_ms: self.processing.retry_initial_delay_ms.unwrap_or(1000),
            retry_max_delay_ms: self.processing.retry_max_delay_ms.unwrap_or(10000),

            // Downloads - use config values if present, otherwise defaults
            concurrent_downloads: self.downloads.concurrent_downloads.unwrap_or(4),
        }
    }
}
