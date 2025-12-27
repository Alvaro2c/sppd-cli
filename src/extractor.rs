use crate::errors::{AppError, AppResult};
use std::fs::File;
use std::io::Read;
use std::path::Path;
use tracing::{debug, info, warn};
use zip::ZipArchive;

/// Extracts all ZIP files from the specified directory into subdirectories.
///
/// For each `{period}.zip` file, extracts its contents into a `{period}/` directory
/// at the same level as the ZIP file.
pub async fn extract_all_zips(directory: &Path) -> AppResult<()> {
    if !directory.exists() {
        return Err(AppError::IoError(format!(
            "Directory does not exist: {}",
            directory.display()
        )));
    }

    let mut zip_files = Vec::new();
    let mut entries = tokio::fs::read_dir(directory).await.map_err(|e| {
        AppError::IoError(format!(
            "Failed to read directory {}: {}",
            directory.display(),
            e
        ))
    })?;

    while let Some(entry) = entries
        .next_entry()
        .await
        .map_err(|e| AppError::IoError(format!("Failed to read directory entry: {e}")))?
    {
        let path = entry.path();
        if path.is_file() && path.extension().and_then(|s| s.to_str()) == Some("zip") {
            zip_files.push(path);
        }
    }

    let mut errors = Vec::new();
    for zip_path in zip_files {
        if let Err(e) = extract_zip(&zip_path).await {
            let error_msg = format!("Failed to extract {}: {}", zip_path.display(), e);
            warn!(
                zip_file = %zip_path.display(),
                error = %e,
                "Failed to extract ZIP file"
            );
            errors.push(error_msg);
        }
    }

    if !errors.is_empty() {
        return Err(AppError::IoError(format!(
            "Failed to extract {} ZIP file(s): {}",
            errors.len(),
            errors.join("; ")
        )));
    }

    Ok(())
}

/// Extracts a single ZIP file into a directory with the same name (without .zip extension).
async fn extract_zip(zip_path: &Path) -> AppResult<()> {
    let zip_file_name = zip_path
        .file_stem()
        .and_then(|s| s.to_str())
        .ok_or_else(|| {
            AppError::InvalidInput(format!("Invalid ZIP file name: {}", zip_path.display()))
        })?;

    let extract_dir = zip_path
        .parent()
        .ok_or_else(|| {
            AppError::InvalidInput(format!(
                "ZIP file has no parent directory: {}",
                zip_path.display()
            ))
        })?
        .join(zip_file_name);

    // Skip if extraction directory already exists
    if extract_dir.exists() {
        debug!(
            zip_file = %zip_path.display(),
            extract_dir = %extract_dir.display(),
            "Skipping extraction, directory already exists"
        );
        return Ok(());
    }

    let extract_dir_display = extract_dir.display().to_string();
    let zip_path_display = zip_path.display().to_string();
    info!(
        zip_file = %zip_path_display,
        extract_dir = %extract_dir_display,
        "Extracting ZIP file"
    );

    // Create extraction directory
    tokio::fs::create_dir_all(&extract_dir).await.map_err(|e| {
        AppError::IoError(format!(
            "Failed to create extraction directory {}: {}",
            extract_dir.display(),
            e
        ))
    })?;

    // Extract ZIP file using blocking I/O in a thread pool
    let zip_path = zip_path.to_path_buf();
    let extract_dir_clone = extract_dir.to_path_buf();

    tokio::task::spawn_blocking(move || {
        // Open and extract ZIP file
        let file = File::open(&zip_path).map_err(|e| {
            AppError::IoError(format!(
                "Failed to open ZIP file {}: {}",
                zip_path.display(),
                e
            ))
        })?;

        let mut archive = ZipArchive::new(file).map_err(|e| {
            AppError::ParseError(format!(
                "Failed to read ZIP archive {}: {}",
                zip_path.display(),
                e
            ))
        })?;

        for i in 0..archive.len() {
            let mut file = archive.by_index(i).map_err(|e| {
                AppError::ParseError(format!(
                    "Failed to read file {} from ZIP {}: {}",
                    i,
                    zip_path.display(),
                    e
                ))
            })?;

            let out_path = match file.enclosed_name() {
                Some(path) => extract_dir_clone.join(path),
                None => continue,
            };

            // Skip directories (they will be created when files are extracted)
            if file.name().ends_with('/') {
                continue;
            }

            // Create parent directories if needed
            if let Some(parent) = out_path.parent() {
                std::fs::create_dir_all(parent).map_err(|e| {
                    AppError::IoError(format!(
                        "Failed to create directory {}: {}",
                        parent.display(),
                        e
                    ))
                })?;
            }

            // Extract file
            let mut out_file = std::fs::File::create(&out_path).map_err(|e| {
                AppError::IoError(format!(
                    "Failed to create file {}: {}",
                    out_path.display(),
                    e
                ))
            })?;

            let mut contents = Vec::new();
            file.read_to_end(&mut contents).map_err(|e| {
                AppError::IoError(format!(
                    "Failed to read file from ZIP {}: {}",
                    zip_path.display(),
                    e
                ))
            })?;

            std::io::Write::write_all(&mut out_file, &contents).map_err(|e| {
                AppError::IoError(format!(
                    "Failed to write file {}: {}",
                    out_path.display(),
                    e
                ))
            })?;
        }

        Ok::<(), AppError>(())
    })
    .await
    .map_err(|e| AppError::IoError(format!("Task join error: {e}")))??;

    info!(
        zip_file = %zip_path_display,
        extract_dir = %extract_dir_display,
        "✓ Extraction completed"
    );

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::extract_all_zips;
    use crate::errors::AppError;
    use std::fs;
    use std::io::Write;
    use std::path::Path;
    use tempfile::TempDir;
    use zip::write::FileOptions;
    use zip::ZipWriter;

    fn create_test_zip(
        zip_path: &Path,
        files: &[(&str, &str)],
    ) -> Result<(), Box<dyn std::error::Error>> {
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

    #[tokio::test]
    async fn test_extract_all_zips_nonexistent_directory() {
        let result = extract_all_zips(Path::new("/nonexistent/directory")).await;
        assert!(result.is_err());
        match result.unwrap_err() {
            AppError::IoError(_) => {}
            _ => panic!("Expected IoError for nonexistent directory"),
        }
    }

    #[tokio::test]
    async fn test_extract_all_zips_empty_directory() {
        let temp_dir = TempDir::new().unwrap();
        let result = extract_all_zips(temp_dir.path()).await;
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn test_extract_all_zips_single_zip() {
        let temp_dir = TempDir::new().unwrap();
        let zip_path = temp_dir.path().join("202501.zip");
        create_test_zip(
            &zip_path,
            &[("file1.xml", "content1"), ("file2.xml", "content2")],
        )
        .unwrap();

        let result = extract_all_zips(temp_dir.path()).await;
        assert!(result.is_ok());

        // Verify extraction directory was created
        let extract_dir = temp_dir.path().join("202501");
        assert!(extract_dir.exists());
        assert!(extract_dir.is_dir());

        // Verify files were extracted
        let file1 = extract_dir.join("file1.xml");
        let file2 = extract_dir.join("file2.xml");
        assert!(file1.exists());
        assert!(file2.exists());

        // Verify file contents
        assert_eq!(fs::read_to_string(&file1).unwrap(), "content1");
        assert_eq!(fs::read_to_string(&file2).unwrap(), "content2");
    }

    #[tokio::test]
    async fn test_extract_all_zips_multiple_zips() {
        let temp_dir = TempDir::new().unwrap();

        // Create first ZIP
        let zip1_path = temp_dir.path().join("202501.zip");
        create_test_zip(&zip1_path, &[("file1.xml", "content1")]).unwrap();

        // Create second ZIP
        let zip2_path = temp_dir.path().join("202502.zip");
        create_test_zip(&zip2_path, &[("file2.xml", "content2")]).unwrap();

        let result = extract_all_zips(temp_dir.path()).await;
        assert!(result.is_ok());

        // Verify both extraction directories were created
        let extract_dir1 = temp_dir.path().join("202501");
        let extract_dir2 = temp_dir.path().join("202502");
        assert!(extract_dir1.exists());
        assert!(extract_dir2.exists());

        // Verify files were extracted
        assert_eq!(
            fs::read_to_string(extract_dir1.join("file1.xml")).unwrap(),
            "content1"
        );
        assert_eq!(
            fs::read_to_string(extract_dir2.join("file2.xml")).unwrap(),
            "content2"
        );
    }

    #[tokio::test]
    async fn test_extract_all_zips_ignores_non_zip_files() {
        let temp_dir = TempDir::new().unwrap();

        // Create a ZIP file
        let zip_path = temp_dir.path().join("202501.zip");
        create_test_zip(&zip_path, &[("file1.xml", "content1")]).unwrap();

        // Create a non-ZIP file
        let txt_path = temp_dir.path().join("readme.txt");
        fs::write(&txt_path, "not a zip").unwrap();

        let result = extract_all_zips(temp_dir.path()).await;
        assert!(result.is_ok());

        // Verify only ZIP was extracted
        let extract_dir = temp_dir.path().join("202501");
        assert!(extract_dir.exists());

        // Non-ZIP file should still exist but not be processed
        assert!(txt_path.exists());
    }

    #[tokio::test]
    async fn test_extract_all_zips_skips_existing_directory() {
        let temp_dir = TempDir::new().unwrap();
        let zip_path = temp_dir.path().join("202501.zip");
        create_test_zip(&zip_path, &[("file1.xml", "content1")]).unwrap();

        // Create extraction directory manually
        let extract_dir = temp_dir.path().join("202501");
        fs::create_dir_all(&extract_dir).unwrap();
        fs::write(extract_dir.join("existing.txt"), "existing").unwrap();

        let result = extract_all_zips(temp_dir.path()).await;
        assert!(result.is_ok());

        // Verify existing file is still there (extraction was skipped)
        assert_eq!(
            fs::read_to_string(extract_dir.join("existing.txt")).unwrap(),
            "existing"
        );
        // Verify ZIP file was not extracted (file1.xml should not exist)
        assert!(!extract_dir.join("file1.xml").exists());
    }

    #[tokio::test]
    async fn test_extract_all_zips_with_subdirectories() {
        let temp_dir = TempDir::new().unwrap();
        let zip_path = temp_dir.path().join("202501.zip");

        // Create ZIP with files in subdirectories
        let file = fs::File::create(&zip_path).unwrap();
        let mut zip = ZipWriter::new(file);
        let options = FileOptions::default().compression_method(zip::CompressionMethod::Stored);

        zip.start_file("subdir/file1.xml", options).unwrap();
        zip.write_all(b"content1").unwrap();
        zip.start_file("subdir/nested/file2.xml", options).unwrap();
        zip.write_all(b"content2").unwrap();
        zip.finish().unwrap();

        let result = extract_all_zips(temp_dir.path()).await;
        assert!(result.is_ok());

        // Verify directory structure was preserved
        let extract_dir = temp_dir.path().join("202501");
        let file1 = extract_dir.join("subdir/file1.xml");
        let file2 = extract_dir.join("subdir/nested/file2.xml");

        assert!(file1.exists());
        assert!(file2.exists());
        assert_eq!(fs::read_to_string(&file1).unwrap(), "content1");
        assert_eq!(fs::read_to_string(&file2).unwrap(), "content2");
    }

    #[tokio::test]
    async fn test_extract_all_zips_returns_error_on_invalid_zip() {
        let temp_dir = TempDir::new().unwrap();

        // Create a file with .zip extension but invalid ZIP content
        let invalid_zip = temp_dir.path().join("invalid.zip");
        fs::write(&invalid_zip, "not a valid zip file").unwrap();

        // Should return an error when extraction fails
        let result = extract_all_zips(temp_dir.path()).await;
        assert!(result.is_err());
        match result.unwrap_err() {
            AppError::IoError(msg) => {
                assert!(msg.contains("Failed to extract"));
                assert!(msg.contains("invalid.zip"));
            }
            _ => panic!("Expected IoError for invalid ZIP"),
        }
    }

    #[tokio::test]
    async fn test_extract_all_zips_returns_error_when_some_fail() {
        let temp_dir = TempDir::new().unwrap();

        // Create one valid ZIP
        let valid_zip = temp_dir.path().join("valid.zip");
        create_test_zip(&valid_zip, &[("file.xml", "content")]).unwrap();

        // Create one invalid ZIP
        let invalid_zip = temp_dir.path().join("invalid.zip");
        fs::write(&invalid_zip, "not a valid zip file").unwrap();

        // Should return an error even though one ZIP succeeded
        let result = extract_all_zips(temp_dir.path()).await;
        assert!(result.is_err());

        // Verify the valid ZIP was still extracted
        let extract_dir = temp_dir.path().join("valid");
        assert!(extract_dir.exists());
    }
}
