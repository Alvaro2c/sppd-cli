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

    for zip_path in zip_files {
        if let Err(e) = extract_zip(&zip_path).await {
            warn!(
                zip_file = %zip_path.display(),
                error = %e,
                "Failed to extract ZIP file"
            );
        }
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
