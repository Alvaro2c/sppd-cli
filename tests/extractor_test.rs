//! Integration tests for extractor module

#[path = "common/mod.rs"]
mod common;

use common::*;
use sppd_cli::extractor;
use sppd_cli::models::ProcurementType;
use std::collections::BTreeMap;
use tempfile::TempDir;

#[tokio::test]
async fn test_extract_all_zips_basic() {
    let temp_dir = TempDir::new().unwrap();
    let extract_dir = temp_dir.path().join("data/tmp/mc");
    std::fs::create_dir_all(&extract_dir).unwrap();

    let zip_path = extract_dir.join("202501.zip");
    create_test_zip(&zip_path, &[("file.xml", "content")]).unwrap();

    let original_dir = std::env::current_dir().unwrap();
    std::env::set_current_dir(temp_dir.path()).unwrap();

    let mut target_links = BTreeMap::new();
    target_links.insert("202501".to_string(), "http://example.com".to_string());
    let procurement_type = ProcurementType::MinorContracts;
    let result = extractor::extract_all_zips(&target_links, &procurement_type, None).await;

    std::env::set_current_dir(original_dir).unwrap();

    assert!(result.is_ok());
    assert!(extract_dir.join("202501").exists());
    assert_eq!(
        std::fs::read_to_string(extract_dir.join("202501/file.xml")).unwrap(),
        "content"
    );
}

#[tokio::test]
async fn test_extract_all_zips_only_targeted() {
    let temp_dir = TempDir::new().unwrap();
    let extract_dir = temp_dir.path().join("data/tmp/mc");
    std::fs::create_dir_all(&extract_dir).unwrap();

    create_test_zip(
        &extract_dir.join("202501.zip"),
        &[("file1.xml", "content1")],
    )
    .unwrap();
    create_test_zip(
        &extract_dir.join("202502.zip"),
        &[("file2.xml", "content2")],
    )
    .unwrap();

    let original_dir = std::env::current_dir().unwrap();
    std::env::set_current_dir(temp_dir.path()).unwrap();

    let mut target_links = BTreeMap::new();
    target_links.insert("202501".to_string(), "http://example.com".to_string());
    let procurement_type = ProcurementType::MinorContracts;
    let result = extractor::extract_all_zips(&target_links, &procurement_type, None).await;

    std::env::set_current_dir(original_dir).unwrap();

    assert!(result.is_ok());
    assert!(extract_dir.join("202501").exists());
    assert!(!extract_dir.join("202502").exists());
}

#[tokio::test]
async fn test_extract_all_zips_error_on_invalid() {
    let temp_dir = TempDir::new().unwrap();
    let extract_dir = temp_dir.path().join("data/tmp/mc");
    std::fs::create_dir_all(&extract_dir).unwrap();
    std::fs::write(extract_dir.join("202501.zip"), "invalid").unwrap();

    let original_dir = std::env::current_dir().unwrap();
    std::env::set_current_dir(temp_dir.path()).unwrap();

    let mut target_links = BTreeMap::new();
    target_links.insert("202501".to_string(), "http://example.com".to_string());
    let procurement_type = ProcurementType::MinorContracts;
    let result = extractor::extract_all_zips(&target_links, &procurement_type, None).await;

    std::env::set_current_dir(original_dir).unwrap();

    assert!(result.is_err());
    match result.unwrap_err() {
        sppd_cli::errors::AppError::IoError(msg) => assert!(msg.contains("Failed to extract")),
        _ => panic!("Expected IoError"),
    }
}
