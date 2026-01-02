//! Tests for config module

use sppd_cli::config::Config;
use std::fs;
use tempfile::TempDir;

#[test]
fn test_config_from_file() {
    let temp_dir = TempDir::new().unwrap();
    let config_path = temp_dir.path().join("sppd.toml");

    let config_content = r#"
[paths]
download_dir_mc = "custom/tmp/mc"
parquet_dir_pt = "custom/parquet/pt"

[processing]
batch_size = 200
max_retries = 5

[downloads]
concurrent_downloads = 8
"#;

    fs::write(&config_path, config_content).unwrap();

    let config = Config::from_file(&config_path).unwrap();

    assert_eq!(
        config
            .paths
            .as_ref()
            .unwrap()
            .download_dir_mc
            .as_ref()
            .unwrap(),
        "custom/tmp/mc"
    );
    assert_eq!(
        config
            .paths
            .as_ref()
            .unwrap()
            .parquet_dir_pt
            .as_ref()
            .unwrap(),
        "custom/parquet/pt"
    );
    assert_eq!(config.batch_size(), 200);
    assert_eq!(config.max_retries(), 5);
    assert_eq!(config.concurrent_downloads(), 8);
}

#[test]
fn test_config_defaults() {
    let config = Config::default();

    assert_eq!(config.batch_size(), 100);
    assert_eq!(config.max_retries(), 3);
    assert_eq!(config.retry_initial_delay_ms(), 1000);
    assert_eq!(config.retry_max_delay_ms(), 10000);
    assert_eq!(config.concurrent_downloads(), 4);
}

#[test]
fn test_config_partial() {
    let temp_dir = TempDir::new().unwrap();
    let config_path = temp_dir.path().join("sppd.toml");

    let config_content = r#"
[processing]
batch_size = 150
"#;

    fs::write(&config_path, config_content).unwrap();

    let config = Config::from_file(&config_path).unwrap();

    // Should use config value for batch_size
    assert_eq!(config.batch_size(), 150);
    // Should use defaults for other values
    assert_eq!(config.max_retries(), 3);
    assert_eq!(config.concurrent_downloads(), 4);
}

#[test]
fn test_config_invalid_toml() {
    let temp_dir = TempDir::new().unwrap();
    let config_path = temp_dir.path().join("sppd.toml");

    let config_content = r#"
[paths
download_dir_mc = "custom/tmp/mc"
"#;

    fs::write(&config_path, config_content).unwrap();

    let result = Config::from_file(&config_path);
    assert!(result.is_err());
}

#[test]
fn test_config_nonexistent_file() {
    let result = Config::from_file("nonexistent.toml");
    assert!(result.is_err());
}
