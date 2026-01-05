use crate::errors::{AppError, AppResult};
use serde::Deserialize;
use std::fs;
use std::path::{Path, PathBuf};

/// Resolved configuration with all values filled in (no Options).
///
/// This struct represents the pipeline defaults and can be deserialized by the TOML
/// loader. All fields have concrete values, making it safe to access directly without unwrapping.
#[derive(Debug, Clone, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct ResolvedConfig {
    // Paths
    pub download_dir_mc: PathBuf,
    pub download_dir_pt: PathBuf,
    pub parquet_dir_mc: PathBuf,
    pub parquet_dir_pt: PathBuf,

    // Processing
    /// Number of XML files processed per chunk during parsing.
    /// This also bounds the peak in-memory DataFrame size.
    pub batch_size: usize,
    /// Number of concurrent XML file reads during parsing.
    pub read_concurrency: usize,
    /// Whether to concatenate per-batch parquet files into a single period file.
    pub concat_batches: bool,
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
            batch_size: 150,
            read_concurrency: 16,
            concat_batches: false,
            max_retries: 3,
            retry_initial_delay_ms: 1000,
            retry_max_delay_ms: 10000,
            concurrent_downloads: 4,
        }
    }
}

/// Configuration that can be loaded from a TOML file.
#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ResolvedConfigFile {
    #[serde(rename = "type")]
    pub procurement_type: String,
    pub start: String,
    pub end: String,
    #[serde(default = "default_cleanup")]
    pub cleanup: bool,
    #[serde(flatten)]
    pub resolved: ResolvedConfig,
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
        if config.resolved.read_concurrency == 0 {
            return Err(AppError::InvalidInput(
                "Read concurrency must be greater than 0".into(),
            ));
        }

        Ok(config)
    }
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
        assert_eq!(config.batch_size, 150);
        assert_eq!(config.read_concurrency, 16);
        assert!(!config.concat_batches);
        assert_eq!(config.concurrent_downloads, 4);
    }

    #[test]
    fn minimal_toml_is_parsed_and_defaults_apply() {
        let mut tmp = NamedTempFile::new().unwrap();
        write!(
            tmp,
            r#"
            type = "mc"
            start = "202301"
            end = "202312"
            "#,
        )
        .unwrap();

        let config = ResolvedConfigFile::from_toml_file(tmp.path()).unwrap();
        assert_eq!(config.procurement_type, "mc");
        assert_eq!(config.start, "202301");
        assert_eq!(config.end, "202312");
        assert!(config.cleanup);
        assert_eq!(config.resolved.max_retries, 3);
        assert_eq!(config.resolved.concurrent_downloads, 4);
    }

    #[test]
    fn missing_required_toml_field_errors() {
        let mut tmp = NamedTempFile::new().unwrap();
        write!(
            tmp,
            r#"
            type = "pt"
            start = "202301"
            "#,
        )
        .unwrap();

        assert!(ResolvedConfigFile::from_toml_file(tmp.path()).is_err());
    }

    #[test]
    fn unknown_key_errors() {
        let mut tmp = NamedTempFile::new().unwrap();
        write!(
            tmp,
            r#"
            type = "pt"
            start = "202301"
            end = "202302"
            extra_flag = true
            "#,
        )
        .unwrap();

        assert!(ResolvedConfigFile::from_toml_file(tmp.path()).is_err());
    }
}
