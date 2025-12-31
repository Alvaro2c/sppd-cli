//! Common test utilities for integration tests

use std::fs;
use std::io::Write;
use std::path::Path;

/// Helper function to create a test XML file in a directory
#[allow(dead_code)]
pub fn create_test_xml_file(path: &Path, content: &str) {
    let parent = path.parent().unwrap();
    fs::create_dir_all(parent).unwrap();
    fs::File::create(path)
        .unwrap()
        .write_all(content.as_bytes())
        .unwrap();
}

/// Helper function to create a test ZIP file with specified files
#[allow(dead_code)]
pub fn create_test_zip(
    zip_path: &Path,
    files: &[(&str, &str)],
) -> Result<(), Box<dyn std::error::Error>> {
    use zip::write::FileOptions;
    use zip::ZipWriter;

    let file = fs::File::create(zip_path)?;
    let mut zip = ZipWriter::new(file);
    let options = FileOptions::default().compression_method(zip::CompressionMethod::Stored);

    for (name, content) in files {
        zip.start_file(*name, options)?;
        zip.write_all(content.as_bytes())?;
    }

    zip.finish()?;
    Ok(())
}

/// Sample XML feed content for testing
#[allow(dead_code)]
pub const SAMPLE_XML_FEED: &str = r#"<?xml version="1.0"?>
<feed>
  <entry>
    <id>test-id-1</id>
    <title>Test Title 1</title>
    <link href="http://example.com/1"/>
    <summary>Summary 1</summary>
    <updated>2023-01-01</updated>
  </entry>
  <entry>
    <id>test-id-2</id>
    <title>Test Title 2</title>
  </entry>
</feed>"#;

/// Sample XML feed with single entry
#[allow(dead_code)]
pub const SINGLE_ENTRY_XML: &str = r#"<?xml version="1.0"?>
<feed>
  <entry>
    <id>single-entry</id>
    <title>Single Entry Title</title>
    <link href="http://example.com/single"/>
  </entry>
</feed>"#;

/// Empty XML feed
#[allow(dead_code)]
pub const EMPTY_XML_FEED: &str = r#"<?xml version="1.0"?>
<feed>
</feed>"#;
