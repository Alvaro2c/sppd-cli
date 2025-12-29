use crate::errors::{AppError, AppResult};
use crate::models::ProcurementType;
use std::collections::BTreeMap;
use std::fs::File;
use std::io::Read;
use std::path::Path;
use tracing::{debug, info, warn};
use zip::ZipArchive;

/// Extracts ZIP files from the specified directory into subdirectories.
///
/// Only extracts ZIP files that correspond to keys in `target_links` (e.g., "202511" -> "202511.zip").
/// For each `{period}.zip` file, extracts its contents into a `{period}/` directory
/// at the same level as the ZIP file.
pub async fn extract_all_zips(
    target_links: &BTreeMap<String, String>,
    procurement_type: &ProcurementType,
) -> AppResult<()> {
    let extract_dir = Path::new(procurement_type.extract_dir());
    if !extract_dir.exists() {
        return Err(AppError::IoError(format!(
            "Directory does not exist: {}",
            extract_dir.display()
        )));
    }

    let mut errors = Vec::new();
    for period in target_links.keys() {
        let zip_path = extract_dir.join(format!("{period}.zip"));
        if !zip_path.exists() {
            warn!(
                zip_file = %zip_path.display(),
                "ZIP file not found, skipping"
            );
            continue;
        }

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
    use crate::models::ProcurementType;
    use std::collections::BTreeMap;
    use std::fs;
    use std::io::Write;
    use std::path::Path;
    use std::sync::Mutex;
    use tempfile::TempDir;
    use zip::write::FileOptions;
    use zip::ZipWriter;

    // Serialize working directory changes to avoid race conditions
    static DIR_LOCK: Mutex<()> = Mutex::new(());

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
    async fn test_extract_all_zips_basic() {
        let _lock = DIR_LOCK.lock().unwrap();
        let temp_dir = TempDir::new().unwrap();
        let extract_dir = temp_dir.path().join("data/tmp/mc");
        fs::create_dir_all(&extract_dir).unwrap();

        let zip_path = extract_dir.join("202501.zip");
        create_test_zip(&zip_path, &[("file.xml", "content")]).unwrap();

        let original_dir = std::env::current_dir().unwrap();
        std::env::set_current_dir(temp_dir.path()).unwrap();

        let mut target_links = BTreeMap::new();
        target_links.insert("202501".to_string(), "http://example.com".to_string());
        let procurement_type = ProcurementType::MinorContracts;
        let result = extract_all_zips(&target_links, &procurement_type).await;

        std::env::set_current_dir(original_dir).unwrap();

        assert!(result.is_ok());
        assert!(extract_dir.join("202501").exists());
        assert_eq!(
            fs::read_to_string(extract_dir.join("202501/file.xml")).unwrap(),
            "content"
        );
    }

    #[tokio::test]
    async fn test_extract_all_zips_only_targeted() {
        let _lock = DIR_LOCK.lock().unwrap();
        let temp_dir = TempDir::new().unwrap();
        let extract_dir = temp_dir.path().join("data/tmp/mc");
        fs::create_dir_all(&extract_dir).unwrap();

        create_test_zip(&extract_dir.join("202501.zip"), &[("file1.xml", "content1")]).unwrap();
        create_test_zip(&extract_dir.join("202502.zip"), &[("file2.xml", "content2")]).unwrap();

        let original_dir = std::env::current_dir().unwrap();
        std::env::set_current_dir(temp_dir.path()).unwrap();

        let mut target_links = BTreeMap::new();
        target_links.insert("202501".to_string(), "http://example.com".to_string());
        let procurement_type = ProcurementType::MinorContracts;
        let result = extract_all_zips(&target_links, &procurement_type).await;

        std::env::set_current_dir(original_dir).unwrap();

        assert!(result.is_ok());
        assert!(extract_dir.join("202501").exists());
        assert!(!extract_dir.join("202502").exists());
    }

    #[tokio::test]
    async fn test_extract_all_zips_error_on_invalid() {
        let _lock = DIR_LOCK.lock().unwrap();
        let temp_dir = TempDir::new().unwrap();
        let extract_dir = temp_dir.path().join("data/tmp/mc");
        fs::create_dir_all(&extract_dir).unwrap();
        fs::write(extract_dir.join("202501.zip"), "invalid").unwrap();

        let original_dir = std::env::current_dir().unwrap();
        std::env::set_current_dir(temp_dir.path()).unwrap();

        let mut target_links = BTreeMap::new();
        target_links.insert("202501".to_string(), "http://example.com".to_string());
        let procurement_type = ProcurementType::MinorContracts;
        let result = extract_all_zips(&target_links, &procurement_type).await;

        std::env::set_current_dir(original_dir).unwrap();

        assert!(result.is_err());
        match result.unwrap_err() {
            AppError::IoError(msg) => assert!(msg.contains("Failed to extract")),
            _ => panic!("Expected IoError"),
        }
    }
}
