use crate::errors::{AppError, AppResult};
use serde::Deserialize;
use std::path::{Path, PathBuf};

/// Configuration structure for the application.
///
/// Supports loading from TOML files with optional sections.
/// Missing sections or fields will use default values.
#[derive(Debug, Clone, Default, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Config {
    pub paths: Option<PathsConfig>,
    pub processing: Option<ProcessingConfig>,
    pub downloads: Option<DownloadsConfig>,
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

    /// Gets the batch size from config, or returns default.
    pub fn batch_size(&self) -> usize {
        self.processing
            .as_ref()
            .and_then(|p| p.batch_size)
            .unwrap_or(100)
    }

    /// Gets the max retries from config, or returns default.
    pub fn max_retries(&self) -> u32 {
        self.processing
            .as_ref()
            .and_then(|p| p.max_retries)
            .unwrap_or(3)
    }

    /// Gets the retry initial delay from config, or returns default.
    pub fn retry_initial_delay_ms(&self) -> u64 {
        self.processing
            .as_ref()
            .and_then(|p| p.retry_initial_delay_ms)
            .unwrap_or(1000)
    }

    /// Gets the retry max delay from config, or returns default.
    pub fn retry_max_delay_ms(&self) -> u64 {
        self.processing
            .as_ref()
            .and_then(|p| p.retry_max_delay_ms)
            .unwrap_or(10000)
    }

    /// Gets the concurrent downloads from config, or returns default.
    pub fn concurrent_downloads(&self) -> usize {
        self.downloads
            .as_ref()
            .and_then(|d| d.concurrent_downloads)
            .unwrap_or(4)
    }
}
