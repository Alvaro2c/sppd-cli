use crate::errors::{AppError, AppResult};
use serde::Deserialize;
use std::fs;
use std::path::{Path, PathBuf};

/// Resolved configuration with all values filled in (no Options).
///
/// This struct represents the pipeline defaults and can be deserialized by the TOML
/// loader. All fields have concrete values, making it safe to access directly without unwrapping.
#[derive(Debug, Clone, Deserialize)]
#[serde(default)]
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

/// Configuration that can be loaded from a TOML file.
#[derive(Debug, Clone, Deserialize)]
#[serde(default)]
pub struct ResolvedConfigFile {
    #[serde(rename = "type")]
    pub procurement_type: String,
    pub start: Option<String>,
    pub end: Option<String>,
    #[serde(default = "default_cleanup")]
    pub cleanup: bool,
    #[serde(flatten)]
    pub resolved: ResolvedConfig,
}

impl Default for ResolvedConfigFile {
    fn default() -> Self {
        Self {
            procurement_type: default_procurement_type(),
            start: None,
            end: None,
            cleanup: default_cleanup(),
            resolved: ResolvedConfig::default(),
        }
    }
}

impl ResolvedConfigFile {
    pub fn from_toml_file(path: &Path) -> AppResult<Self> {
        let contents = fs::read_to_string(path)?;
        let config: ResolvedConfigFile = toml::from_str(&contents)
            .map_err(|e| AppError::InvalidInput(format!("Failed to parse config: {e}")))?;

        if config.resolved.batch_size == 0 {
            return Err(AppError::InvalidInput(
                "Batch size must be greater than 0".into(),
            ));
        }

        Ok(config)
    }
}

fn default_procurement_type() -> String {
    "public-tenders".to_string()
}

fn default_cleanup() -> bool {
    true
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write;
    use tempfile::NamedTempFile;

    #[test]
    fn default_config_values() {
        let config = ResolvedConfig::default();
        assert_eq!(config.batch_size, 100);
        assert_eq!(config.concurrent_downloads, 4);
    }

    #[test]
    fn toml_file_parses_custom_values() {
        let mut tmp = NamedTempFile::new().unwrap();
        write!(
            tmp,
            r#"
            type = "mc"
            batch_size = 42
            concurrent_downloads = 2
            cleanup = false
            "#,
        )
        .unwrap();

        let config = ResolvedConfigFile::from_toml_file(tmp.path()).unwrap();
        assert_eq!(config.procurement_type, "mc");
        assert_eq!(config.resolved.batch_size, 42);
        assert_eq!(config.resolved.concurrent_downloads, 2);
        assert!(!config.cleanup);
    }

    #[test]
    fn toml_file_batch_size_zero_is_error() {
        let mut tmp = NamedTempFile::new().unwrap();
        write!(
            tmp,
            r#"
            batch_size = 0
            "#,
        )
        .unwrap();

        assert!(ResolvedConfigFile::from_toml_file(tmp.path()).is_err());
    }
}
