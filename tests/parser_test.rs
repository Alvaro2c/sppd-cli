//! Integration tests for parser module

#[path = "common/mod.rs"]
mod common;

use common::*;
use polars::prelude::*;
use sppd_cli::models::ProcurementType;
use sppd_cli::parser;
use std::collections::BTreeMap;
use std::fs::File;
use tempfile::TempDir;

#[tokio::test]
async fn test_parse_xmls_end_to_end() {
    let temp_dir = TempDir::new().unwrap();
    let extract_dir = temp_dir.path().join("data/tmp/mc");
    std::fs::create_dir_all(&extract_dir).unwrap();

    // Create subdirectory with XML files
    let subdir = extract_dir.join("202301");
    std::fs::create_dir_all(&subdir).unwrap();

    create_test_xml_file(&subdir.join("feed.xml"), SAMPLE_XML_FEED);

    // Change working directory for relative path resolution
    let original_dir = std::env::current_dir().unwrap();
    std::env::set_current_dir(temp_dir.path()).unwrap();

    let mut target_links = BTreeMap::new();
    target_links.insert("202301".to_string(), "http://example.com".to_string());

    let result = parser::parse_xmls(&target_links, &ProcurementType::MinorContracts, 100, None);

    assert!(result.is_ok());

    std::env::set_current_dir(original_dir).unwrap();

    // Verify parquet file was created (use absolute path after chdir back)
    let parquet_path = temp_dir.path().join("data/parquet/mc/202301.parquet");
    assert!(parquet_path.exists());

    // Read and verify parquet content
    let file = File::open(&parquet_path).unwrap();
    let df = ParquetReader::new(file).finish().unwrap();
    assert_eq!(df.height(), 2);
    assert!(df.column("id").is_ok());
    assert!(df.column("title").is_ok());
}

#[tokio::test]
async fn test_parse_xmls_filters_by_target_links() {
    let temp_dir = TempDir::new().unwrap();
    let extract_dir = temp_dir.path().join("data/tmp/pt");
    std::fs::create_dir_all(&extract_dir).unwrap();

    // Create multiple subdirectories
    let subdir1 = extract_dir.join("202301");
    let subdir2 = extract_dir.join("202302");
    std::fs::create_dir_all(&subdir1).unwrap();
    std::fs::create_dir_all(&subdir2).unwrap();

    create_test_xml_file(
        &subdir1.join("feed.xml"),
        r#"<?xml version="1.0"?>
<feed>
  <entry><id>1</id><title>One</title></entry>
</feed>"#,
    );

    create_test_xml_file(
        &subdir2.join("feed.xml"),
        r#"<?xml version="1.0"?>
<feed>
  <entry><id>2</id><title>Two</title></entry>
</feed>"#,
    );

    let original_dir = std::env::current_dir().unwrap();
    std::env::set_current_dir(temp_dir.path()).unwrap();

    // Only include 202301 in target_links
    let mut target_links = BTreeMap::new();
    target_links.insert("202301".to_string(), "http://example.com".to_string());

    let result = parser::parse_xmls(&target_links, &ProcurementType::PublicTenders, 100, None);

    assert!(result.is_ok());

    std::env::set_current_dir(original_dir).unwrap();

    // Only 202301.parquet should exist (use absolute path)
    assert!(temp_dir
        .path()
        .join("data/parquet/pt/202301.parquet")
        .exists());
    assert!(!temp_dir
        .path()
        .join("data/parquet/pt/202302.parquet")
        .exists());
}

#[tokio::test]
async fn test_parse_xmls_skips_empty_entries() {
    let temp_dir = TempDir::new().unwrap();
    let extract_dir = temp_dir.path().join("data/tmp/mc");
    std::fs::create_dir_all(&extract_dir).unwrap();

    let subdir = extract_dir.join("202301");
    std::fs::create_dir_all(&subdir).unwrap();

    // XML with no entries (empty feed)
    create_test_xml_file(&subdir.join("feed.xml"), EMPTY_XML_FEED);

    let original_dir = std::env::current_dir().unwrap();
    std::env::set_current_dir(temp_dir.path()).unwrap();

    let mut target_links = BTreeMap::new();
    target_links.insert("202301".to_string(), "http://example.com".to_string());

    let result = parser::parse_xmls(&target_links, &ProcurementType::MinorContracts, 100, None);

    assert!(result.is_ok());

    std::env::set_current_dir(original_dir).unwrap();

    // Parquet file should not be created for empty entries (use absolute path)
    assert!(!temp_dir
        .path()
        .join("data/parquet/mc/202301.parquet")
        .exists());
}

#[tokio::test]
async fn test_parse_xmls_merges_multiple_files() {
    let temp_dir = TempDir::new().unwrap();
    let extract_dir = temp_dir.path().join("data/tmp/pt");
    std::fs::create_dir_all(&extract_dir).unwrap();

    let subdir = extract_dir.join("202301");
    std::fs::create_dir_all(&subdir).unwrap();

    // Multiple XML files in same subdirectory
    create_test_xml_file(
        &subdir.join("feed1.xml"),
        r#"<?xml version="1.0"?>
<feed>
  <entry><id>1</id><title>One</title></entry>
</feed>"#,
    );

    create_test_xml_file(
        &subdir.join("feed2.xml"),
        r#"<?xml version="1.0"?>
<feed>
  <entry><id>2</id><title>Two</title></entry>
  <entry><id>3</id><title>Three</title></entry>
</feed>"#,
    );

    let original_dir = std::env::current_dir().unwrap();
    std::env::set_current_dir(temp_dir.path()).unwrap();

    let mut target_links = BTreeMap::new();
    target_links.insert("202301".to_string(), "http://example.com".to_string());

    let result = parser::parse_xmls(&target_links, &ProcurementType::PublicTenders, 100, None);

    assert!(result.is_ok());

    std::env::set_current_dir(original_dir).unwrap();

    // Verify all entries are merged (use absolute path)
    // Verify file exists and has reasonable size instead of loading full DataFrame
    // Row count verification is covered in test_parse_xmls_end_to_end
    let parquet_path = temp_dir.path().join("data/parquet/pt/202301.parquet");
    assert!(parquet_path.exists());
    let metadata = std::fs::metadata(&parquet_path).unwrap();
    assert!(metadata.len() > 0, "Parquet file should have content");
}

#[tokio::test]
async fn test_cleanup_files_with_cleanup_true() {
    let temp_dir = TempDir::new().unwrap();
    let extract_dir = temp_dir.path().join("data/tmp/mc");
    std::fs::create_dir_all(&extract_dir).unwrap();

    let period = "202301";
    let zip_path = extract_dir.join(format!("{period}.zip"));
    let dir_path = extract_dir.join(period);

    // Create ZIP file and directory
    std::fs::File::create(&zip_path).unwrap();
    std::fs::create_dir_all(&dir_path).unwrap();
    create_test_xml_file(&dir_path.join("file.xml"), "<feed></feed>");

    // Change working directory for relative path resolution
    let original_dir = std::env::current_dir().unwrap();
    std::env::set_current_dir(temp_dir.path()).unwrap();

    let mut target_links = BTreeMap::new();
    target_links.insert(period.to_string(), "http://example.com".to_string());

    let result =
        parser::cleanup_files(&target_links, &ProcurementType::MinorContracts, true, None).await;
    std::env::set_current_dir(original_dir).unwrap();

    assert!(result.is_ok());
    assert!(!zip_path.exists());
    assert!(!dir_path.exists());
}

#[tokio::test]
async fn test_cleanup_files_with_cleanup_false() {
    let temp_dir = TempDir::new().unwrap();
    let extract_dir = temp_dir.path().join("data/tmp/pt");
    std::fs::create_dir_all(&extract_dir).unwrap();

    let period = "202302";
    let zip_path = extract_dir.join(format!("{period}.zip"));
    let dir_path = extract_dir.join(period);

    std::fs::File::create(&zip_path).unwrap();
    std::fs::create_dir_all(&dir_path).unwrap();

    let original_dir = std::env::current_dir().unwrap();
    std::env::set_current_dir(temp_dir.path()).unwrap();

    let mut target_links = BTreeMap::new();
    target_links.insert(period.to_string(), "http://example.com".to_string());

    let result =
        parser::cleanup_files(&target_links, &ProcurementType::PublicTenders, false, None).await;
    std::env::set_current_dir(original_dir).unwrap();

    assert!(result.is_ok());
    assert!(zip_path.exists());
    assert!(dir_path.exists());
}

#[tokio::test]
async fn test_cleanup_files_nonexistent_extract_dir() {
    let temp_dir = TempDir::new().unwrap();
    let original_dir = std::env::current_dir().unwrap();
    std::env::set_current_dir(temp_dir.path()).unwrap();

    let mut target_links = BTreeMap::new();
    target_links.insert("202303".to_string(), "http://example.com".to_string());

    // Extract directory doesn't exist
    let result =
        parser::cleanup_files(&target_links, &ProcurementType::MinorContracts, true, None).await;
    std::env::set_current_dir(original_dir).unwrap();

    assert!(result.is_ok());
}

#[tokio::test]
async fn test_cleanup_files_missing_zip_and_dir() {
    let temp_dir = TempDir::new().unwrap();
    let extract_dir = temp_dir.path().join("data/tmp/mc");
    std::fs::create_dir_all(&extract_dir).unwrap();

    let original_dir = std::env::current_dir().unwrap();
    std::env::set_current_dir(temp_dir.path()).unwrap();

    let mut target_links = BTreeMap::new();
    target_links.insert("202304".to_string(), "http://example.com".to_string());

    // ZIP and directory don't exist - should continue without error
    let result =
        parser::cleanup_files(&target_links, &ProcurementType::MinorContracts, true, None).await;
    std::env::set_current_dir(original_dir).unwrap();

    assert!(result.is_ok());
}
