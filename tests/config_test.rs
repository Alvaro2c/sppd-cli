//! Tests for config module

use sppd_cli::config::{Config, ResolvedConfig};
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
    let resolved = config.resolve(None);

    assert_eq!(
        resolved.download_dir_mc,
        std::path::PathBuf::from("custom/tmp/mc")
    );
    assert_eq!(
        resolved.parquet_dir_pt,
        std::path::PathBuf::from("custom/parquet/pt")
    );
    assert_eq!(resolved.batch_size, 200);
    assert_eq!(resolved.max_retries, 5);
    assert_eq!(resolved.concurrent_downloads, 8);
}

#[test]
fn test_config_defaults() {
    let resolved = ResolvedConfig::default();

    assert_eq!(resolved.batch_size, 100);
    assert_eq!(resolved.max_retries, 3);
    assert_eq!(resolved.retry_initial_delay_ms, 1000);
    assert_eq!(resolved.retry_max_delay_ms, 10000);
    assert_eq!(resolved.concurrent_downloads, 4);
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
    let resolved = config.resolve(None);

    // Should use config value for batch_size
    assert_eq!(resolved.batch_size, 150);
    // Should use defaults for other values
    assert_eq!(resolved.max_retries, 3);
    assert_eq!(resolved.concurrent_downloads, 4);
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

#[test]
fn test_resolve_precedence_cli_over_config() {
    let temp_dir = TempDir::new().unwrap();
    let config_path = temp_dir.path().join("sppd.toml");

    let config_content = r#"
[processing]
batch_size = 150
"#;

    fs::write(&config_path, config_content).unwrap();

    let config = Config::from_file(&config_path).unwrap();
    // CLI arg should override config file
    let resolved = config.resolve(Some(200));

    assert_eq!(resolved.batch_size, 200);
}

#[test]
fn test_resolve_precedence_config_over_default() {
    let temp_dir = TempDir::new().unwrap();
    let config_path = temp_dir.path().join("sppd.toml");

    let config_content = r#"
[processing]
batch_size = 150
"#;

    fs::write(&config_path, config_content).unwrap();

    let config = Config::from_file(&config_path).unwrap();
    let resolved = config.resolve(None);

    assert_eq!(resolved.batch_size, 150);
}

#[test]
fn test_resolve_with_empty_config() {
    let config = Config::default();
    let resolved = config.resolve(None);

    // Should use all defaults
    assert_eq!(resolved.batch_size, 100);
    assert_eq!(resolved.max_retries, 3);
    assert_eq!(resolved.concurrent_downloads, 4);
    assert_eq!(
        resolved.download_dir_mc,
        std::path::PathBuf::from("data/tmp/mc")
    );
    assert_eq!(
        resolved.download_dir_pt,
        std::path::PathBuf::from("data/tmp/pt")
    );
}
