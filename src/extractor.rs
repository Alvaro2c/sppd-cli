use crate::errors::{AppError, AppResult};
use crate::models::ProcurementType;
use indicatif::{ProgressBar, ProgressStyle};
use std::collections::BTreeMap;
use std::fs::File;
use std::path::Path;
use tracing::{debug, info, warn};
use zip::ZipArchive;

/// Extracts ZIP files from the specified directory into subdirectories.
///
/// This function processes ZIP files that correspond to periods in `target_links`.
/// For each period (e.g., "202301"), it looks for a corresponding ZIP file (`202301.zip`)
/// in the extraction directory and extracts its contents into a subdirectory named
/// after the period (`202301/`).
///
/// # Behavior
///
/// - **Skip existing**: If an extraction directory already exists for a period, that
///   ZIP file is skipped.
/// - **Missing files**: Missing ZIP files are logged as warnings but don't fail the
///   operation.
/// - **Progress tracking**: A progress bar is displayed during extraction.
///
/// # Arguments
///
/// * `target_links` - Map of period strings to URLs (used to determine which ZIPs to extract)
/// * `procurement_type` - Procurement type determining the extraction directory
///
/// # Directory Structure
///
/// For a period "202301", the function expects:
/// - Input: `{extract_dir}/202301.zip`
/// - Output: `{extract_dir}/202301/` (contains extracted XML/Atom files)
///
/// # Errors
///
/// Returns an error if:
/// - The extraction directory doesn't exist
/// - ZIP file extraction fails for any file
///
/// # Example
///
/// ```no_run
/// use sppd_cli::{extractor, models::ProcurementType};
/// use std::collections::BTreeMap;
///
/// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
/// let mut links = BTreeMap::new();
/// links.insert("202301".to_string(), "https://example.com/202301.zip".to_string());
/// extractor::extract_all_zips(&links, &ProcurementType::PublicTenders).await?;
/// // Extracts data/tmp/pt/202301.zip -> data/tmp/pt/202301/
/// # Ok(())
/// # }
/// ```
pub async fn extract_all_zips(
    target_links: &BTreeMap<String, String>,
    procurement_type: &ProcurementType,
) -> AppResult<()> {
    let extract_dir = procurement_type.extract_dir();
    if !extract_dir.exists() {
        return Err(AppError::IoError(format!(
            "Directory does not exist: {}",
            extract_dir.display()
        )));
    }

    // Collect ZIP files that need extraction
    let mut zips_to_extract = Vec::new();
    let mut missing_zips = Vec::new();

    for period in target_links.keys() {
        let zip_path = extract_dir.join(format!("{period}.zip"));
        if !zip_path.exists() {
            missing_zips.push((period.clone(), zip_path));
            continue;
        }

        // Check if extraction directory already exists
        let extract_dir_path = zip_path.parent().unwrap().join(period);

        if !extract_dir_path.exists() {
            zips_to_extract.push((period.clone(), zip_path));
        }
    }

    let total_zips = zips_to_extract.len();
    let skipped_count = target_links.len() - total_zips - missing_zips.len();

    if total_zips == 0 {
        info!(
            total = target_links.len(),
            skipped = skipped_count,
            missing = missing_zips.len(),
            "All ZIP files already extracted, skipping extraction"
        );
        return Ok(());
    }

    // Log warnings for missing ZIP files
    for (period, zip_path) in &missing_zips {
        warn!(
            zip_file = %zip_path.display(),
            period = period,
            "ZIP file not found, skipping"
        );
    }

    // Create progress bar
    let pb = ProgressBar::new(total_zips as u64);
    pb.set_style(
        ProgressStyle::default_bar()
            .template(
                "{spinner:.green} [{elapsed_precise}] [{wide_bar:.cyan/blue}] {pos}/{len} {msg}",
            )
            .unwrap()
            .progress_chars("#>-"),
    );

    info!(
        total = total_zips,
        skipped = skipped_count,
        missing = missing_zips.len(),
        "Starting extraction"
    );

    let mut errors = Vec::new();
    for (_period, zip_path) in zips_to_extract {
        let filename = zip_path
            .file_name()
            .and_then(|n| n.to_str())
            .unwrap_or("unknown")
            .to_string();

        // Update progress bar message
        pb.set_message(format!("Extracting {filename}..."));

        if let Err(e) = extract_zip(&zip_path).await {
            let error_msg = format!("Failed to extract {}: {}", zip_path.display(), e);
            warn!(
                zip_file = %zip_path.display(),
                error = %e,
                "Failed to extract ZIP file"
            );
            errors.push(error_msg);
        }

        // Update progress bar
        pb.inc(1);
        pb.set_message(format!("Completed {filename}"));
    }

    pb.finish_with_message(format!("Extracted {total_zips} ZIP file(s)"));

    if !errors.is_empty() {
        return Err(AppError::IoError(format!(
            "Failed to extract {} ZIP file(s): {}",
            errors.len(),
            errors.join("; ")
        )));
    }

    if skipped_count > 0 {
        debug!(skipped = skipped_count, "Skipped already extracted files");
    }

    info!(
        extracted = total_zips,
        skipped = skipped_count,
        missing = missing_zips.len(),
        "Extraction completed"
    );

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

            // Extract file using streaming copy (no intermediate buffer)
            let mut out_file = std::fs::File::create(&out_path).map_err(|e| {
                AppError::IoError(format!(
                    "Failed to create file {}: {}",
                    out_path.display(),
                    e
                ))
            })?;

            std::io::copy(&mut file, &mut out_file).map_err(|e| {
                AppError::IoError(format!(
                    "Failed to copy file from ZIP {} to {}: {}",
                    zip_path.display(),
                    out_path.display(),
                    e
                ))
            })?;
        }

        Ok::<(), AppError>(())
    })
    .await
    .map_err(|e| AppError::IoError(format!("Task join error: {e}")))??;

    Ok(())
}
