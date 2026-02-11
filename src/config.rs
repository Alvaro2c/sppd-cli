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
    /// Directory for downloaded minor contracts ZIP files
    pub download_dir_mc: PathBuf,
    /// Directory for downloaded public tenders ZIP files
    pub download_dir_pt: PathBuf,
    /// Directory for processed minor contracts Parquet files
    pub parquet_dir_mc: PathBuf,
    /// Directory for processed public tenders Parquet files
    pub parquet_dir_pt: PathBuf,

    // Processing
    /// Number of XML files processed per chunk during parsing.
    /// This also bounds the peak in-memory DataFrame size.
    pub batch_size: usize,
    /// Number of concurrent XML file reads during parsing.
    pub read_concurrency: usize,
    /// Number of threads for the XML parsing rayon pool.
    /// When set to 0 (default), automatically uses available_parallelism().
    /// In Docker/constrained environments, set to the container's CPU limit.
    pub parser_threads: usize,
    /// Whether to concatenate per-batch parquet files into a single period file.
    pub concat_batches: bool,
    /// Whether to include the raw ContractFolderStatus XML in the parquet output.
    pub keep_cfs_raw_xml: bool,
    /// Maximum number of retry attempts for failed downloads
    pub max_retries: u32,
    /// Initial delay in milliseconds before the first retry
    pub retry_initial_delay_ms: u64,
    /// Maximum delay in milliseconds between retries
    pub retry_max_delay_ms: u64,

    // Downloads
    /// Number of concurrent download tasks
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
            parser_threads: 0, // 0 means auto-detect via available_parallelism()
            concat_batches: false,
            keep_cfs_raw_xml: false,
            max_retries: 3,
            retry_initial_delay_ms: 1000,
            retry_max_delay_ms: 10000,
            concurrent_downloads: 4,
        }
    }
}

/// Configuration that can be loaded from a TOML file.
///
/// Deserializes required fields (type, start, end) and optional pipeline configuration.
/// The parser rejects unknown keys to catch typos, and validates that batch_size and
/// read_concurrency are greater than 0.
#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ResolvedConfigFile {
    /// Procurement type: `"public-tenders"`, `"pt"`, `"pub"`, `"minor-contracts"`, `"mc"`, or `"min"`
    #[serde(rename = "type")]
    pub procurement_type: String,
    /// Start period in `YYYY` or `YYYYMM` format
    pub start: String,
    /// End period in `YYYY` or `YYYYMM` format
    pub end: String,
    /// Whether to clean up temporary ZIP and extracted files (defaults to `true`)
    #[serde(default = "default_cleanup")]
    pub cleanup: bool,
    /// Flattened resolved configuration with pipeline defaults
    #[serde(flatten)]
    pub resolved: ResolvedConfig,
}

impl ResolvedConfigFile {
    /// Loads and validates configuration from a TOML file.
    ///
    /// Deserializes the TOML file and ensures all required fields are present.
    /// Validates that batch_size and read_concurrency are greater than 0.
    /// Rejects unknown keys to prevent typos from being silently ignored.
    ///
    /// # Arguments
    ///
    /// * `path` - Path to the TOML configuration file
    ///
    /// # Returns
    ///
    /// Returns the loaded configuration if all validations pass.
    ///
    /// # Errors
    ///
    /// Returns `InvalidInput` if the TOML is malformed, required fields are missing,
    /// unknown keys are present, or batch_size/read_concurrency are not positive.
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
        assert!(!config.keep_cfs_raw_xml);
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
        assert!(!config.resolved.keep_cfs_raw_xml);
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
